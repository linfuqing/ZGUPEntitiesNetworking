using System;
using Unity.Collections;
using Unity.Networking.Transport;
using NetworkWriter = Unity.Collections.DataStreamWriter;

namespace Unity.Networking.Transport
{
    public static partial class NetworkUtility
    {
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