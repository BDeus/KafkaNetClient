using System;
using System.Collections.Generic;
using KafkaNet.Model;
using KafkaNet.Protocol;

namespace KafkaNet
{
    internal static class Extensions
    {
        public static FetchRequest ToFetchRequest(this List<Fetch> fetches, ConsumerOptions options)
        {
            return new FetchRequest
            {
                MaxWaitTime = (int)Math.Min(int.MaxValue, options.MaxWaitTimeForMinimumBytes.TotalMilliseconds),
                MinBytes = options.MinimumBytes,
                Fetches = fetches
            };
        }

        public static FetchRequest ToFetchRequest(this Fetch fetch, ConsumerOptions options)
        {
            return new FetchRequest
            {
                MaxWaitTime = (int)Math.Min(int.MaxValue, options.MaxWaitTimeForMinimumBytes.TotalMilliseconds),
                MinBytes = options.MinimumBytes,
                Fetches = new List<Fetch>() { fetch }
            };
        }

        public static void HandleResponseErrors(this FetchResponse response, Fetch request)
        {
            switch ((ErrorResponseCode)response.Error)
            {
                case ErrorResponseCode.NoError:
                    return;

                case ErrorResponseCode.OffsetOutOfRange:
                    throw new OffsetOutOfRangeException("FetchResponse indicated we requested an offset that is out of range.  Requested Offset:{0}", request.Offset) { FetchRequest = request };
                case ErrorResponseCode.BrokerNotAvailable:
                case ErrorResponseCode.ConsumerCoordinatorNotAvailableCode:
                case ErrorResponseCode.LeaderNotAvailable:
                case ErrorResponseCode.NotLeaderForPartition:
                    throw new InvalidMetadataException("FetchResponse indicated we may have mismatched metadata.  ErrorCode:{0}", response.Error) { ErrorCode = response.Error };
                default:
                    throw new KafkaApplicationException("FetchResponse returned error condition.  ErrorCode:{0}", response.Error) { ErrorCode = response.Error };
            }
        }
    }
}
