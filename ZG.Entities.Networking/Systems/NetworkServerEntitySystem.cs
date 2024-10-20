using System;
using System.Runtime.InteropServices;
using Unity.Burst;
using Unity.Burst.Intrinsics;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Entities;
using Unity.Jobs;
using Unity.Networking.Transport;

namespace ZG
{
    [Flags]
    public enum NetworkServerEntityComponentFlag
    { 
        Packed = 0x01, 
        SendAlways = 0x02
    }

    public struct NetworkServerEntityComponent : IBufferElementData
    {
        public NetworkServerEntityComponentFlag flag;
        
        public uint handle;
        public int channel;
        public int rpcType;

        public int additionalIDComponentTypeIndex;

        public int maskIDComponentTypeIndex;

        public int componentTypeIndex;
    }

    public struct NetworkServerEntityID : IBufferElementData
    {
        public int value;
    }

    public struct NetworkServerEntityBufferRange : IBufferElementData
    {
        public int offset;
        public int length;
    }

    [StructLayout(LayoutKind.Sequential, Size = 1)]
    public struct NetworkServerEntityBuffer : IBufferElementData
    {
        public byte value;
    }

    public struct NetworkServerEntityChannel : IBufferElementData
    {
        public int capacity;
        public NetworkPipeline pipeline;
    }

    [AutoCreateIn("Server"), BurstCompile, CreateAfter(typeof(NetworkServerSystem)), CreateAfter(typeof(NetworkRPCSystem)), UpdateBefore(typeof(NetworkRPCSystem))]
    public partial struct NetworkServerEntitySystem : ISystem
    {
        private struct Result
        {
            public Entity entity;
            public int componentIndex;
            public int idStartIndex;
            public int additionalIDCount;
            public int maskIDCount;
        }
        
        private struct Collect
        {
            public int componentTypeCount;

            public BurstCompatibleTypeArrayReadOnly componentTypes;

            public ArchetypeChunk chunk;

            [ReadOnly] 
            public NativeArray<Entity> entityArray;

            [ReadOnly] 
            public BufferAccessor<NetworkServerEntityComponent> components;

            public BufferAccessor<NetworkServerEntityID> ids;

            public BufferAccessor<NetworkServerEntityBufferRange> bufferRanges;

            public BufferAccessor<NetworkServerEntityBuffer> buffers;

            public NativeQueue<Result>.ParallelWriter results;

            public unsafe void Execute(int index)
            {
                Result result;
                result.entity = entityArray[index];
                
                var bufferRanges = this.bufferRanges[index];
                var components = this.components[index];
                int numComponents = components.Length, i, j;
                bufferRanges.Resize(numComponents, NativeArrayOptions.ClearMemory);

                var ids = this.ids[index];
                ids.Clear();
                
                void* source, destination;
                bool isSendAlways;
                int typeSize, bufferLength;
                TypeIndex typeIndex;
                NetworkServerEntityComponent component;
                DynamicBuffer<NetworkServerEntityBuffer> buffer;
                for(i = 0; i < numComponents; ++i)
                {
                    component = components[i];
                    if (component.componentTypeIndex < 0 || component.componentTypeIndex >= componentTypeCount)
                        continue;

                    ref var componentType = ref componentTypes[component.componentTypeIndex];
                    if(!chunk.Has(ref componentType))
                        continue;

                    isSendAlways = (component.flag & NetworkServerEntityComponentFlag.SendAlways) ==
                                   NetworkServerEntityComponentFlag.SendAlways;
                    
                    typeIndex = componentType.GetTypeIndex();
                    typeSize = TypeManager.GetTypeInfo(typeIndex).TypeSize;

                    if (typeIndex.IsBuffer)
                    {
                        var bufferAccessor = chunk.GetUntypedBufferAccessor(ref componentType);
                        source = bufferAccessor.GetUnsafeReadOnlyPtrAndLength(index, out bufferLength);

                        /*if (bufferLength > 0)
                            UnityEngine.Debug.LogError("fuck!");*/
                    }
                    else
                    {
                        var array = chunk.GetDynamicComponentDataArrayReinterpret<byte>(ref componentType,
                            typeSize);
                        source = (byte*)array.GetUnsafeReadOnlyPtr() +  index * typeSize;
                        
                        bufferLength = 1;
                    }
                    
                    bufferLength *= typeSize;

                    buffer = buffers[index];

                    ref var bufferRange = ref bufferRanges.ElementAt(i);
                    if (!isSendAlways && 
                        bufferLength > 0 && 
                        (bufferRange.length < 1 || bufferRange.length + bufferRange.offset > buffer.Length))
                    {
                        bufferRange.offset = buffer.Length;
                        bufferRange.length = bufferLength;
                        
                        buffer.ResizeUninitialized(bufferRange.offset + bufferRange.length);
                        destination = UnsafeUtility.AddressOf(ref buffer.ElementAt(bufferRange.offset));
                    }
                    else
                    {
                        if (bufferLength == bufferRange.length)
                        {
                            if (bufferLength < 1)
                                continue;
                            
                            destination = UnsafeUtility.AddressOf(ref buffer.ElementAt(bufferRange.offset));

                            if (UnsafeUtility.MemCmp(source, destination, bufferLength) == 0)
                            {
                                if (isSendAlways)
                                    destination = null;
                                else
                                    continue;
                            }
                        }
                        else
                        {
                            if (bufferRange.length > 0)
                            {
                                buffer.RemoveRange(bufferRange.offset, bufferRange.length);

                                for (j = 0; j < numComponents; ++j)
                                {
                                    ref var temp = ref bufferRanges.ElementAt(j);

                                    if (temp.offset > bufferRange.offset)
                                        temp.offset -= bufferRange.length;
                                }
                            }

                            bufferRange.offset = buffer.Length;
                            bufferRange.length = bufferLength;
                            
                            if(bufferLength < 1)
                                continue;

                            buffer.ResizeUninitialized(bufferRange.offset + bufferRange.length);

                            destination = UnsafeUtility.AddressOf(ref buffer.ElementAt(bufferRange.offset));
                        }
                         
                        result.componentIndex = i;
                        result.idStartIndex = ids.Length;
                        result.additionalIDCount = __GetIDCount(component.additionalIDComponentTypeIndex, index, ref ids);
                        result.maskIDCount = __GetIDCount(component.maskIDComponentTypeIndex, index, ref ids);

                        results.Enqueue(result);
                        
                        if(destination == null)
                            continue;
                    }

                    UnsafeUtility.MemCpy(destination, source, bufferLength);
                }
            }

            private unsafe int __GetIDCount(int componentTypeIndex, int index, ref DynamicBuffer<NetworkServerEntityID> ids)
            {
                if (componentTypeIndex >= 0 &&
                    componentTypeIndex < componentTypeCount)
                {
                    var componentType = componentTypes[componentTypeIndex];
                    var typeIndex = componentType.GetTypeIndex();
                    if (typeIndex.IsBuffer)
                    {
                        var bufferAccessor = chunk.GetUntypedBufferAccessor(ref componentType);

                        var ptr = bufferAccessor.GetUnsafeReadOnlyPtrAndLength(index, out int length);
                        if (length > 0)
                        {
                            int typeSize = TypeManager.GetTypeInfo(typeIndex).TypeSize;
                            if (typeSize == UnsafeUtility.SizeOf<NetworkServerEntityID>())
                                ids.AddRange(CollectionHelper.ConvertExistingDataToNativeArray<NetworkServerEntityID>(ptr, length, Allocator.None, true));
                            else
                            {
                                for (int i = 0; i < length; ++i)
                                    ids.Add(UnsafeUtility.ReadArrayElementWithStride<NetworkServerEntityID>(ptr, i, typeSize));
                            }

                            return length;
                        }
                    }
                    else
                    {
                        int typeSize = TypeManager.GetTypeInfo(typeIndex).TypeSize;
                        var bytes = chunk.GetDynamicComponentDataArrayReinterpret<byte>(ref componentType,
                            typeSize);
                        ids.Add(UnsafeUtility.ReadArrayElementWithStride<NetworkServerEntityID>(bytes.GetUnsafeReadOnlyPtr(), index, typeSize));

                        return 1;
                    }
                }

                return 0;
            }
        }

        [BurstCompile]
        private struct CollectEx : IJobChunk
        {
            public int componentTypeCount;

            public BurstCompatibleTypeArrayReadOnly componentTypes;

            [ReadOnly] 
            public EntityTypeHandle entityType;

            [ReadOnly] 
            public BufferTypeHandle<NetworkServerEntityComponent> componentType;

            public BufferTypeHandle<NetworkServerEntityID> idType;

            public BufferTypeHandle<NetworkServerEntityBufferRange> bufferRangeType;

            public BufferTypeHandle<NetworkServerEntityBuffer> bufferType;

            public NativeQueue<Result>.ParallelWriter results;

            public void Execute(in ArchetypeChunk chunk, int unfilteredChunkIndex, bool useEnabledMask,
                in v128 chunkEnabledMask)
            {
                Collect collect;
                collect.componentTypeCount = componentTypeCount;
                collect.componentTypes = componentTypes;
                collect.chunk = chunk;
                collect.entityArray = chunk.GetNativeArray(entityType);
                collect.components = chunk.GetBufferAccessor(ref componentType);
                collect.ids = chunk.GetBufferAccessor(ref idType);
                collect.bufferRanges = chunk.GetBufferAccessor(ref bufferRangeType);
                collect.buffers = chunk.GetBufferAccessor(ref bufferType);
                collect.results = results;

                var iterator = new ChunkEntityEnumerator(useEnabledMask, chunkEnabledMask, chunk.Count);
                while (iterator.NextEntityIndex(out int i))
                    collect.Execute(i);
            }
        }

        [BurstCompile]
        private struct Send : IJob
        {
            public StreamCompressionModel model;
            
            [ReadOnly] 
            public NativeArray<NetworkServerEntityChannel> channels;
            
            [ReadOnly]
            public ComponentLookup<NetworkIdentity> identities;

            [ReadOnly]
            public BufferLookup<NetworkServerEntityComponent> components;

            [ReadOnly] 
            public BufferLookup<NetworkServerEntityID> ids;

            [ReadOnly]
            public BufferLookup<NetworkServerEntityBufferRange> bufferRanges;

            [ReadOnly]
            public BufferLookup<NetworkServerEntityBuffer> buffers;

            //public NetworkDriver driver;
            public NetworkRPCCommander rpcCommander;

            public NativeQueue<Result> results;

            public void Execute()
            {
                int value;
                NetworkServerEntityChannel channel;
                NativeArray<uint> ids;
                NativeArray<byte> buffer;
                NetworkServerEntityBufferRange bufferRange;
                NetworkServerEntityComponent component;
                DataStreamWriter stream;
                while (results.TryDequeue(out var result))
                {
                    component = components[result.entity][result.componentIndex];
                    channel = channels[component.channel];
                    if (!rpcCommander.BeginCommand(identities[result.entity].id, channel.capacity, channel.pipeline, out stream))
                        continue;

                    stream.WritePackedUInt(component.handle);
                    
                    bufferRange = bufferRanges[result.entity][result.componentIndex];
                    buffer = buffers[result.entity].AsNativeArray().Reinterpret<byte>().GetSubArray(bufferRange.offset, bufferRange.length);
                    if ((component.flag & NetworkServerEntityComponentFlag.Packed) ==
                        NetworkServerEntityComponentFlag.Packed &&
                        (bufferRange.length & 0x3) == 0)
                    {
                        foreach (var packedValue in buffer.Reinterpret<uint>(1))
                            stream.WritePackedUInt(packedValue, model);
                    }
                    else
                        stream.WriteBytes(buffer);

                    ids = this.ids[result.entity].AsNativeArray().Reinterpret<uint>();
                    value = rpcCommander.EndCommandRPC(
                        component.rpcType, 
                        stream, 
                        ids.GetSubArray(result.idStartIndex, result.additionalIDCount), 
                        ids.GetSubArray(result.idStartIndex + result.additionalIDCount, result.maskIDCount));
                    if(value < 0)
                        UnityEngine.Debug.LogError($"[EndRPC]{(Unity.Networking.Transport.Error.StatusCode)value}");
                }
            }
        }

        private int __componentTypeCount;

        private BurstCompatibleTypeArrayReadOnly __componentTypes;

        private EntityQuery __group;
        //private EntityQuery __managerGroup;
        private EntityQuery __controllerGroup;

        private EntityTypeHandle __entityType;

        private BufferTypeHandle<NetworkServerEntityComponent> __componentType;

        private BufferTypeHandle<NetworkServerEntityID> __idType;

        private BufferTypeHandle<NetworkServerEntityBufferRange> __bufferRangeType;

        private BufferTypeHandle<NetworkServerEntityBuffer> __bufferType;

        private ComponentLookup<NetworkIdentity> __identities;

        private BufferLookup<NetworkServerEntityComponent> __components;

        private BufferLookup<NetworkServerEntityID> __ids;

        private BufferLookup<NetworkServerEntityBufferRange> __bufferRanges;

        private BufferLookup<NetworkServerEntityBuffer> __buffers;

        private NativeQueue<Result> __results;

        private NativeHashMap<TypeIndex, int> __typeIndices;

        public static int GetOrCreateComponentTypeIndex(in WorldUnmanaged world, in TypeIndex typeIndex)
        {
            bool isNew = false;
            ref var system = ref world.GetExistingSystemUnmanaged<NetworkServerEntitySystem>();
            if (!system.__typeIndices.TryGetValue(typeIndex, out int index))
            {
                isNew = true;
                
                index = system.__componentTypeCount++;

                system.__typeIndices[typeIndex] = index;
            }

            ref var state = ref world.GetExistingSystemState<NetworkServerEntitySystem>();
            ComponentType componentType;
            componentType.TypeIndex = typeIndex;
            componentType.AccessModeType = ComponentType.AccessMode.ReadOnly;

            var typeHandle = state.GetDynamicComponentTypeHandle(componentType);
            if (isNew && index == 0)
            {
                for (int i = 0; i < BurstCompatibleTypeArrayReadOnly.LENGTH; ++i)
                    system.__componentTypes[i] = typeHandle;
            }
            else
                system.__componentTypes[index] = typeHandle;
            
            return index;
        }

        [BurstCompile]
        public void OnCreate(ref SystemState state)
        {
            using (var builder = new EntityQueryBuilder(Allocator.Temp))
                __group = builder
                    .WithAll<NetworkIdentity, NetworkServerEntityComponent>()
                    .WithAllRW<NetworkServerEntityID>()
                    .WithAllRW<NetworkServerEntityBufferRange, NetworkServerEntityBuffer>()
                    .WithOptions(EntityQueryOptions.IncludeDisabledEntities)
                    .Build(ref state);

            //__managerGroup = NetworkServerManager.GetEntityQuery(ref state);
            __controllerGroup = NetworkRPCController.GetEntityQuery(ref state);

            __entityType = state.GetEntityTypeHandle();
            __componentType = state.GetBufferTypeHandle<NetworkServerEntityComponent>(true);
            __idType = state.GetBufferTypeHandle<NetworkServerEntityID>();
            __bufferRangeType = state.GetBufferTypeHandle<NetworkServerEntityBufferRange>();
            __bufferType = state.GetBufferTypeHandle<NetworkServerEntityBuffer>();
            __identities = state.GetComponentLookup<NetworkIdentity>(true);
            __components = state.GetBufferLookup<NetworkServerEntityComponent>(true);
            __ids = state.GetBufferLookup<NetworkServerEntityID>(true);
            __bufferRanges = state.GetBufferLookup<NetworkServerEntityBufferRange>(true);
            __buffers = state.GetBufferLookup<NetworkServerEntityBuffer>(true);

            __results = new NativeQueue<Result>(Allocator.Persistent);

            __typeIndices = new NativeHashMap<TypeIndex, int>(1, Allocator.Persistent);

            state.EntityManager.AddComponent<NetworkServerEntityChannel>(state.SystemHandle);
        }

        //[BurstCompile]
        public void OnDestroy(ref SystemState state)
        {
            __results.Dispose();

            __typeIndices.Dispose();
        }

        [BurstCompile]
        public void OnUpdate(ref SystemState state)
        {
            if (__componentTypeCount < 1)
                return;
            
            CollectEx collect;
            collect.componentTypeCount = __componentTypeCount;
            collect.componentTypes = __componentTypes.UpdateAsRef(ref state);
            collect.entityType = __entityType.UpdateAsRef(ref state);
            collect.componentType = __componentType.UpdateAsRef(ref state);
            collect.idType = __idType.UpdateAsRef(ref state);
            collect.bufferRangeType = __bufferRangeType.UpdateAsRef(ref state);
            collect.bufferType = __bufferType.UpdateAsRef(ref state);
            collect.results = __results.AsParallelWriter();

            var jobHandle = collect.ScheduleParallelByRef(__group, state.Dependency);

            //var manager = __managerGroup.GetSingleton<NetworkServerManager>();
            var controller = __controllerGroup.GetSingleton<NetworkRPCController>();
            
            Send send;
            send.model = StreamCompressionModel.Default;
            send.channels = state.EntityManager.GetBuffer<NetworkServerEntityChannel>(state.SystemHandle, true).AsNativeArray();
            send.identities = __identities.UpdateAsRef(ref state);
            send.components = __components.UpdateAsRef(ref state);
            send.ids = __ids.UpdateAsRef(ref state);
            send.bufferRanges = __bufferRanges.UpdateAsRef(ref state);
            send.buffers = __buffers.UpdateAsRef(ref state);
            //send.driver = manager.server.driver;
            send.rpcCommander = controller.commander;
            send.results = __results;

            //ref var managerJobManager = ref manager.lookupJobManager;
            ref var controllerJobManager = ref controller.lookupJobManager;

            jobHandle = JobHandle.CombineDependencies(jobHandle, 
                //managerJobManager.readWriteJobHandle,
                controllerJobManager.readWriteJobHandle);
            jobHandle = send.ScheduleByRef(jobHandle);

            //managerJobManager.readWriteJobHandle = jobHandle;
            controllerJobManager.readWriteJobHandle = jobHandle;

            state.Dependency = jobHandle;
        }
    }
}