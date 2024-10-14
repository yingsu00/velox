/*
* Copyright (c) Facebook, Inc. and its affiliates.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/


#include "velox/exec/OptimizedPartitionedOutput.h"

#include "velox/type/Type.h"
#include "velox/exec/Task.h"
#include "velox/common/base/SimdUtil.h"

namespace facebook::velox::exec {
    namespace {
//        __attribute__((target("default")))
        inline void countPartitionSizes(const std::vector<uint32_t> &partitions, std::vector<vector_size_t> &counts) {
            for (auto i = 0; i < partitions.size(); i++) {
                counts[partitions[i]]++;
            }
        }

//        __attribute__((target("default")))
        inline void prefixSum(std::vector<vector_size_t> &offsets, uint32_t numPartitions) {
            for (uint32_t i = 1; i <= numPartitions; i++) {
                offsets[i] += offsets[i - 1];
            }
        }

        inline void writeInt32(OutputStream *out, int32_t value) {
            out->write(reinterpret_cast<char *>(&value), sizeof(value));
        }

        inline void writeInt64(OutputStream *out, int64_t value) {
            out->write(reinterpret_cast<char *>(&value), sizeof(value));
        }
    }

    PartitioningVectorSerializer::PartitioningVectorSerializer(int32_t numDestinations,
                                                               core::PartitionFunction *partitionFunction,
                                                               const std::weak_ptr<exec::OutputBufferManager> &bufferManager,
                                                               const std::function<void()> &bufferReleaseFn,
                                                               memory::MemoryPool *pool)
            : numDestinations_(numDestinations),
              bufferManager_(bufferManager),
              bufferReleaseFn_(bufferReleaseFn),
              partitionFunction_(partitionFunction),
              bytesBuffered_(0),
              rowsBuffered_(0),
              pool_(pool) {
        beginOffsets_.resize(numDestinations_);
//        offsets_.resize(numDestinations_);
    }

    void PartitioningVectorSerializer::append(
            RowVectorPtr &input) {
        partitionFunction_->partition(*input, partitions_);

        // offsets_ will be reused and size() doesn't decrease.
        while (offsets_.size() <= partitionedPages_.size()) {
            // Insert one offsets vector first
            offsets_.emplace_back(numDestinations_, 0);
            VELOX_CHECK_EQ(offsets_.size(), partitionedPages_.size() + 1);
        }

        auto &offsets = offsets_[partitionedPages_.size()];
//        std::memset(offsets.data(), 0, 4 * numDestinations_);
        countPartitionSizes(partitions_, offsets);
        prefixSum(offsets, numDestinations_);
        std::memcpy(&beginOffsets_[1], &offsets_[0], 4 * (numDestinations_ - 1));

        partitionedPages_.emplace_back(partitionRowVectorInPlace(input));
        bytesBuffered_ += input->inMemoryBytes();
        rowsBuffered_ += input->size();
    }


    std::map<uint32_t, std::unique_ptr<SerializedPage>> PartitioningVectorSerializer::flush() {
        if (partitionedPages_.empty()) {
            return std::map<uint32_t, std::unique_ptr<SerializedPage>>();
        }

        tempVectors_.resize(partitionedPages_.size());

        std::vector<IOBufOutputStream> outputStreams;
        for (uint32_t destination = 0; destination < numDestinations_; destination++) {
            outputStreams.emplace_back(
                    *pool_,
                    bufferManager_.lock()->newListener().get(),
                    bytesBuffered_ / numDestinations_
            );
        }

        flushRowVectors(partitionedPages_, outputStreams);

        std::map<uint32_t, std::unique_ptr<SerializedPage>> serializedPages;

        vector_size_t beginOffset = 0;
        for (uint32_t destination = 0; destination < numDestinations_; destination++) {
            auto &outputStream = outputStreams[destination];
            auto offsets = offsets_[destination];
            const int64_t flushedBytes = outputStream.tellp();
            if (flushedBytes > 0) {
                auto flushedRows = offsets[destination] - beginOffset;
                serializedPages[destination] = std::make_unique<SerializedPage>(outputStream.getIOBuf(bufferReleaseFn_),
                                                                                nullptr, flushedRows);
                beginOffset = offsets[destination];
            }
        }

        bytesBuffered_ = 0;
        partitionedPages_.clear();
        offsets_.clear();

        return serializedPages;
    }

    int64_t PartitioningVectorSerializer::bytesBuffered() {
        return bytesBuffered_;
    }

    int64_t PartitioningVectorSerializer::rowsBuffered() {
        return rowsBuffered_;
    }

    VectorPtr PartitioningVectorSerializer::partitionInPlace(VectorPtr input) {
        auto encoding = input->encoding();
        switch (encoding) {
            case VectorEncoding::Simple::FLAT:
                return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
                        PartitioningVectorSerializer::partitionFlatVectorInPlace,
                        input->typeKind(),
                        input);
            case VectorEncoding::Simple::ROW:
                return partitionRowVectorInPlace(std::dynamic_pointer_cast<RowVector>(input));
            case VectorEncoding::Simple::BIASED:
            case VectorEncoding::Simple::ARRAY:
            case VectorEncoding::Simple::MAP:
            case VectorEncoding::Simple::LAZY:
            case VectorEncoding::Simple::DICTIONARY:
            case VectorEncoding::Simple::SEQUENCE:
                VELOX_UNSUPPORTED("Unsupported vector encoding for OptimizedPartitionedOutput: ", encoding);
                break;
            default:
                VELOX_UNREACHABLE("Invalid vector encoding for OptimizedPartitionedOutput: ", encoding);

        }
        return input;
    }

    RowVectorPtr PartitioningVectorSerializer::partitionRowVectorInPlace(RowVectorPtr vector) {
        for (int i = 0; i < vector->childrenSize(); i++) {
            VectorPtr childVec = vector->childAt(i);
            partitionInPlace(childVec);
        }
        return vector;
    }

    template<TypeKind kind>
    VectorPtr
    PartitioningVectorSerializer::partitionFlatVectorInPlace(VectorPtr vector) {
        using T = typename TypeTraits<kind>::NativeType;
        auto *flatVector = vector->as<FlatVector<T>>();
        auto *values = flatVector->mutableRawValues();
        auto numValues = vector->size();

        auto &offsets = offsets_[partitionedPages_.size()];
//        std::memset(offsets.data(), 0, 4 * numDestinations_);
//        countPartitionSizes(partitions_, offsets);
//        prefixSum(offsets, numDestinations_);
//        std::memcpy(&beginOffsets_[1], &offsets_[0], 4 * (numDestinations_ - 1));

//        for (auto i = 0; i < numValues; i++) {
//            uint32_t partition = partitions_[i];
//            vector_size_t offset = beginOffsets_[partition];
//            while (i != offset) {
//                auto nextPartition = partitions_[offset];
//                std::memcpy(values + offset * typeWidth, values + i * typeWidth, typeWidth);
//                beginOffsets_[partition]++;
//                offset = beginOffsets_[nextPartition];
//            }
//            beginOffsets_[partition]++;
//        }

        for (auto partition = 0; partition < numDestinations_; partition++) {
            auto offset = beginOffsets_[partition];  // 0
            auto endOffset = offsets[partition];  // 4
            while (offset < endOffset) {
                uint32_t p = partitions_[offset];  // 1
                while (p != partition) {
                    auto destinationOffset = beginOffsets_[p]++;  // 4
                    std::swap(values[destinationOffset], values[offset]); // swap 3 and 5
                    p = partitions_[offset];   // 0
                }
                offset = ++beginOffsets_[partition];
            }
        }

        return vector;
    }

    void PartitioningVectorSerializer::flushVectors(const std::vector<VectorPtr> &vectors,
                                                    std::vector<IOBufOutputStream> &outputStreams) {
        VELOX_CHECK_GT(vectors.size(), 0);

        auto encoding = vectors[0]->encoding();
        auto typeKind = vectors[0]->typeKind();

        switch (encoding) {
            case VectorEncoding::Simple::FLAT:
                return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
                        PartitioningVectorSerializer::flushFlatVectors,
                        typeKind,
                        vectors,
                        outputStreams);
            case VectorEncoding::Simple::ROW:
                return flushRowVectors(vectors, outputStreams);
            case VectorEncoding::Simple::BIASED:
            case VectorEncoding::Simple::ARRAY:
            case VectorEncoding::Simple::MAP:
            case VectorEncoding::Simple::LAZY:
            case VectorEncoding::Simple::DICTIONARY:
            case VectorEncoding::Simple::SEQUENCE:
                VELOX_UNSUPPORTED("Unsupported vector encoding for OptimizedPartitionedOutput: ", encoding);
                break;
            default:
                VELOX_UNREACHABLE("Invalid vector encoding for OptimizedPartitionedOutput: ", encoding);

        }
    }

    void
    PartitioningVectorSerializer::flushRowVectors(const std::vector<VectorPtr> &rowVectors,
                                                  std::vector<IOBufOutputStream> &outputStreams) {
        VELOX_CHECK_GT(rowVectors.size(), 0);

        auto numColumns = rowVectors[0]->as<RowVector>()->childrenSize();
        for (uint32_t column = 0; column < numColumns; column++) {
            for (int i = 0; i < rowVectors.size(); i++) {
                tempVectors_[i] = rowVectors[i]->as<RowVector>()->childAt(column);
            }
            // flush column to output
            flushVectors(tempVectors_, outputStreams);
        }
    }

    template<TypeKind kind>
    void
    PartitioningVectorSerializer::flushFlatVectors(const std::vector<VectorPtr> &vectors,
                                                   std::vector<IOBufOutputStream> &outputStreams) {
        for (int destination = 0; destination < numDestinations_; destination++) {
            writeInt32(&outputStreams_[destination], rowCounts_[destination]);
        }

        // Flush nulls

        // Flush values
        for (int i = 0; i < vectors.size(); i++) {
            VectorPtr vector = vectors[i];
            using T = typename TypeTraits<kind>::NativeType;
            auto typeWidth = sizeof(T);
            auto *flatVector = vector->as<FlatVector<T>>();
            auto *values = flatVector->rawValues();
            auto offsets = offsets_[i];
            auto lastOffset = 0;
            for (int destination = 0; destination < numDestinations_; destination++) {
                auto offset = offsets[destination];
                auto numValues = offset - lastOffset;
                outputStreams_[destination].write(reinterpret_cast<const char *>(&values[lastOffset]),
                                                  numValues * typeWidth);
                lastOffset = offset;
            }
        }
    }

//    std::unordered_map<std::string, RuntimeCounter> PartitioningVectorSerializer::runtimeStats() {
//        std::unordered_map<std::string, RuntimeCounter> map;
//        map.insert(
//                {{"compressedBytes",
//                         RuntimeCounter(stats_.compressedBytes, RuntimeCounter::Unit::kBytes)},
//                 {"compressionInputBytes",
//                         RuntimeCounter(
//                                 stats_.compressionInputBytes, RuntimeCounter::Unit::kBytes)},
//                 {"compressionSkippedBytes",
//                         RuntimeCounter(
//                                 stats_.compressionSkippedBytes, RuntimeCounter::Unit::kBytes)}});
//        return map;
//    }

    OptimizedPartitionedOutput::OptimizedPartitionedOutput(
            int32_t operatorId,
            DriverCtx *ctx,
            const std::shared_ptr<const core::PartitionedOutputNode> &planNode,
            bool eagerFlush)
            : Operator(
            ctx,
            planNode->outputType(),
            operatorId,
            planNode->id(),
            "OptimizedPartitionedOutput"),
              taskId_(operatorCtx_->taskId()),
              keyChannels_(toChannels(planNode->inputType(), planNode->keys())),
              outputChannels_(calculateOutputChannels(
                      planNode->inputType(),
                      planNode->outputType(),
                      planNode->outputType())),
              numDestinations_(planNode->numPartitions()),
              partitionFunction_(
                      numDestinations_ == 1
                      ? nullptr
                      : planNode->partitionFunctionSpec().create(numDestinations_)),
              replicateNullsAndAny_(planNode->isReplicateNullsAndAny()),
              eagerFlush_(eagerFlush),
              bufferManager_(OutputBufferManager::getInstance()),
            // NOTE: 'bufferReleaseFn_' holds a reference on the associated task to
            // prevent it from deleting while there are output buffers being accessed
            // out of the partitioned output buffer manager such as in Prestissimo,
            // the http server holds the buffers while sending the data response.
              bufferReleaseFn_([task = operatorCtx_->task()]() {}),
              maxBufferedBytes_(ctx->task->queryCtx()
                                        ->queryConfig()
                                        .maxPartitionedOutputBufferSize()),
              maxSerializedPageBytes_(std::max<uint64_t>(
                      kMinDestinationSize,  // 60KB
                      std::min<uint64_t>(1 << 20, maxBufferedBytes_ /*32MB*/ / numDestinations_))),
              pool_(pool()),
              serializer_(numDestinations_, partitionFunction_.get(), bufferManager_, bufferReleaseFn_, pool_) {
        if (!planNode->isPartitioned()) {
            VELOX_USER_CHECK_EQ(numDestinations_, 1);
        }
        if (numDestinations_ == 1) {
            VELOX_USER_CHECK(keyChannels_.empty());
            VELOX_USER_CHECK_NULL(partitionFunction_);
        }
//        pool_ = pool();

//        for (uint32_t destination = 0; destination < numDestinations_; destination++) {
//            outputStreams_.emplace_back(
//                    *pool_,
//                    bufferManager_.lock()->newListener().get(),
//                    kMinMessageSize);   // TODO: know the size
//        }
    }

    BlockingReason OptimizedPartitionedOutput::isBlocked(ContinueFuture *future) {
        if (blockingReason_ != BlockingReason::kNotBlocked) {
            *future = std::move(future_);
            blockingReason_ = BlockingReason::kNotBlocked;
            return BlockingReason::kWaitForConsumer;
        }
        return BlockingReason::kNotBlocked;
    }

    void OptimizedPartitionedOutput::addInput(RowVectorPtr input) {
        // TODO: replicateNullsAndAny_

        if (serializer_.bytesBuffered() + input->inMemoryBytes() >= maxSerializedPageBytes_ * numDestinations_) {
            flush();
        }

        serializer_.append(input);
    }

    RowVectorPtr OptimizedPartitionedOutput::getOutput() {
        if (finished_) {
            return nullptr;
        }

        blockingReason_ = BlockingReason::kNotBlocked;

        if (noMoreInput_ || serializer_.bytesBuffered() >= maxSerializedPageBytes_ * numDestinations_) {
            flush();
        }

        if (noMoreInput_) {
            finished_ = true;
        }
        // The input is fully processed, drop the reference to allow reuse.
        input_ = nullptr;
//        output_ = nullptr;
        return nullptr;
    }


    bool OptimizedPartitionedOutput::isFinished() {
        return finished_;
    }


    void OptimizedPartitionedOutput::flush() {
        auto flushedBytes = serializer_.bytesBuffered();
        auto flushedRows = serializer_.rowsBuffered();

        auto serializedPages = serializer_.flush();

        for (uint32_t destination = 0; destination < numDestinations_; destination++) {
            auto &serializedPage = serializedPages[destination];

            if (serializedPage) {
                bool blocked = bufferManager_.lock()->enqueue(
                        taskId_,
                        destination,
                        std::move(serializedPage),
                        &future_);
                if (blocked) {
                    blockingReason_ = BlockingReason::kWaitForConsumer;
                }

                auto lockedStats = stats_.wlock();
                lockedStats->addOutputVector(flushedBytes, flushedRows);
            }
        }
    }
}