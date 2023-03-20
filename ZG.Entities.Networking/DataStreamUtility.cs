using System;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Networking.Transport;

namespace ZG
{
    public enum NetworkMessageType : byte
    {
        Connect,
        Disconnect, 
        RPC,
        Register,
        Unregister,

        Unknown
    }

    public enum NetworkPipelineType
    {
        Null, 
        ReliableSequence,
        UnreliableSequence,
        FragmentationReliableSequence,
        FragmentationUnreliableSequence,
        Fragmentation,
    }

    public static partial class DataStreamUtility
    {

        public static DataStreamWriter CreateWriter(in NativeArray<byte> bytes, in IntPtr sendHandleData)
        {
            var writer = new DataStreamWriter(bytes);
            writer.m_SendHandleData = sendHandleData;

            return writer;
        }

        public static IntPtr GetSendHandleData(in this DataStreamWriter writer)
        {
            return writer.m_SendHandleData;
        }

        public static void SetSendHandleData(ref this DataStreamWriter writer, IntPtr ptr)
        {
            writer.m_SendHandleData = ptr;
        }

        public static unsafe bool ReadBool(this ref DataStreamReader reader)
        {
            return reader.ReadByte() != 0;
        }

        public static unsafe T Read<T>(ref DataStreamReader reader) where T : unmanaged
        {
            T data;
            var array = NativeArrayUnsafeUtility.ConvertExistingDataToNativeArray<byte>(&data, UnsafeUtility.SizeOf<T>(), Allocator.None);
#if ENABLE_UNITY_COLLECTIONS_CHECKS
            NativeArrayUnsafeUtility.SetAtomicSafetyHandle(ref array, AtomicSafetyHandle.GetTempMemoryHandle());
#endif
            reader.ReadBytes(array);
            return data;
        }

        public static sbyte ReadSByte(this ref DataStreamReader reader)
        {
            return Read<sbyte>(ref reader);
        }

        public static ulong ReadPackedULong(this ref DataStreamReader reader) => reader.ReadPackedULong(StreamCompressionModel.Default);

        public static uint ReadPackedUInt(this ref DataStreamReader reader) => reader.ReadPackedUInt(StreamCompressionModel.Default);

        public static void WritePackedUInt(this ref DataStreamWriter writer, uint value) => writer.WritePackedUInt(value, StreamCompressionModel.Default);

        public static void WritePackedULong(this ref DataStreamWriter writer, ulong value) => writer.WritePackedULong(value, StreamCompressionModel.Default);

        public static unsafe bool Write<T>(ref DataStreamWriter writer, T value) where T : unmanaged
        {
            var array = NativeArrayUnsafeUtility.ConvertExistingDataToNativeArray<byte>(&value, UnsafeUtility.SizeOf<T>(), Allocator.None);
#if ENABLE_UNITY_COLLECTIONS_CHECKS
            NativeArrayUnsafeUtility.SetAtomicSafetyHandle(ref array, AtomicSafetyHandle.GetTempMemoryHandle());
#endif
            return writer.WriteBytes(array);
        }

        public static unsafe bool WriteSByte(this ref DataStreamWriter writer, sbyte value)
        {
            return Write(ref writer, value);
        }

        public static unsafe bool WriteBool(this ref DataStreamWriter writer, bool value)
        {
            return writer.WriteByte(value ? (byte)1 : (byte)0);
        }

        public static NetworkPipeline CreatePipeline(this ref NetworkDriver driver, NetworkPipelineType type)
        {
            switch(type)
            {
                case NetworkPipelineType.ReliableSequence:
                    return driver.CreatePipeline(typeof(ReliableSequencedPipelineStage));
                case NetworkPipelineType.UnreliableSequence:
                    return driver.CreatePipeline(typeof(UnreliableSequencedPipelineStage));
                case NetworkPipelineType.FragmentationReliableSequence:
                    return driver.CreatePipeline(typeof(FragmentationPipelineStage), typeof(ReliableSequencedPipelineStage));
                case NetworkPipelineType.FragmentationUnreliableSequence:
                    return driver.CreatePipeline(typeof(FragmentationPipelineStage), typeof(UnreliableSequencedPipelineStage));
                case NetworkPipelineType.Fragmentation:
                    return driver.CreatePipeline(typeof(FragmentationPipelineStage));
            }

            return driver.CreatePipeline(typeof(NullPipelineStage));
        }
    }
}