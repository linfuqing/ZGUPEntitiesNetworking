using System;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;

namespace Unity.Networking.Transport
{
    public struct NetworkSend : IEquatable<NetworkSend>
    {
        public NetworkPipeline pipeline;
        public NetworkConnection connection;

        public bool Equals(NetworkSend other)
        {
            return pipeline == other.pipeline && connection == other.connection;
        }

        public override int GetHashCode()
        {
            return pipeline.GetHashCode() ^ connection.GetHashCode();
        }
    }

    public static partial class NetworkUtility
    {
        /*public static int GetSendSize()
        {
            return UnsafeUtility.SizeOf<NetworkDriver.Concurrent.PendingSend>();
        }
        
        public static unsafe void CloneSend(this ref DataStreamWriter writer, ref NativeArray<byte> values)
        {
            var pendingSend = (NetworkDriver.Concurrent.PendingSend*)((byte*)values.GetUnsafePtr());
            *pendingSend = *(NetworkDriver.Concurrent.PendingSend*)(void*)writer.m_SendHandleData;

            var target =
                NativeArrayUnsafeUtility.ConvertExistingDataToNativeArray<byte>(
                    writer.AsNativeArray().GetUnsafePtr(),
                    writer.Capacity, 
                    Allocator.Invalid);
            
#if ENABLE_UNITY_COLLECTIONS_CHECKS
            NativeArrayUnsafeUtility.SetAtomicSafetyHandle(ref target, NativeArrayUnsafeUtility.GetAtomicSafetyHandle(values));
#endif

            writer = new DataStreamWriter(target);
            writer.m_SendHandleData = (IntPtr)pendingSend;
        }

        public static unsafe NetworkSend GetSend(this in DataStreamWriter writer)
        {
            var pendingSend = (NetworkDriver.Concurrent.PendingSend*)(void*)writer.m_SendHandleData;
            NetworkSend send;
            send.pipeline = pendingSend->Pipeline;
            send.connection = pendingSend->Connection;

            return send;
        }*/

        public static int PayloadCapacity(
            this in NetworkDriver.Concurrent driver,
            in NetworkPipeline pipe)
        {
            var pipelinePayloadCapacity = driver.m_PipelineProcessor.PayloadCapacity(pipe);
            if(pipelinePayloadCapacity == 0)
                return NetworkParameterConstants.MTU - driver.m_PacketPadding - driver.MaxHeaderSize(pipe);

            return pipelinePayloadCapacity;
        }

        public static int PayloadCapacity(
            this in NetworkDriver driver,
            in NetworkPipeline pipe)
        {
            return driver.ToConcurrent().PayloadCapacity(pipe);
        }

        public static int MaxPayloadCapacity(this in NetworkDriver.Concurrent driver)
        {
            int maxPayloadCapacity = NetworkParameterConstants.MTU - driver.m_PacketPadding - driver.m_PipelineProcessor.m_MaxPacketHeaderSize - 1;
            foreach (var pipelineImpl in driver.m_PipelineProcessor.m_Pipelines)
                maxPayloadCapacity = Math.Max(maxPayloadCapacity, pipelineImpl.payloadCapacity);

            return maxPayloadCapacity;
        }

        public static int MaxPayloadCapacity(this in NetworkDriver driver)
        {
            return driver.ToConcurrent().MaxPayloadCapacity();
        }

        public static int PipelineCount(this in NetworkDriver.Concurrent driver)
        {
            return driver.m_PipelineProcessor.m_Pipelines.Length;
        }

        public static int PipelineCount(this in NetworkDriver driver)
        {
            return driver.ToConcurrent().PipelineCount();
        }
    }
}