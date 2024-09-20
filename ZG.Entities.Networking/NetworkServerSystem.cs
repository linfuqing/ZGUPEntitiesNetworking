using System;
using System.Diagnostics;
using Unity.Jobs;
using Unity.Burst;
using Unity.Entities;
using Unity.Collections;
using Unity.Mathematics;
using Unity.Networking.Transport;
using Unity.Networking.Transport.Error;
using Unity.Collections.LowLevel.Unsafe;

namespace ZG
{
    public struct NetworkServerMessage : System.IComparable<NetworkServerMessage>
    {
        public uint type;
        public int index;
        public NetworkConnection connection;
        internal UnsafeBlock _block;

        public DataStreamReader stream => _block.isCreated ? new DataStreamReader(_block.AsArray<byte>()) : default;

        public int CompareTo(NetworkServerMessage other)
        {
            return index.CompareTo(other.index);
        }
    }

    public struct NetworkServerMessageManager
    {
        private NativeParallelMultiHashMap<uint, NetworkServerMessage>.ReadOnly __buffers;

        public NetworkServerMessageManager(in NativeParallelMultiHashMap<uint, NetworkServerMessage>.ReadOnly buffers)
        {
            __buffers = buffers;
        }

        public void GetIDs(ref NativeList<uint> ids)
        {
            uint id;
            var enumerator = __buffers.GetEnumerator();
            while (enumerator.MoveNext())
            {
                id = enumerator.Current.Key;
                if (ids.IndexOf(id) == -1)
                    ids.Add(id);
            }
        }

        public bool Receive(uint id, ref NativeList<NetworkServerMessage> messages)
        {
            int index = messages.Length;
            if (__buffers.TryGetFirstValue(id, out var message, out var iterator))
            {
                do
                {
                    messages.Add(message);
                } while (__buffers.TryGetNextValue(out message, ref iterator));

                messages.AsArray().GetSubArray(index, messages.Length - index).Sort();

                return true;
            }

            return false;
        }

    }

    public struct NetworkServer
    {
        private struct Command
        {
            public NetworkConnection connection;
            public NetworkPipeline pipeline;
            public UnsafeBlock block;

            public int Send(ref NetworkDriver.Concurrent driver)
            {
                int result = driver.BeginSend(pipeline, connection, out var writer);
                if (result < 0)
                    return result;

                writer.WriteBytes(block.AsArray<byte>());
                return driver.EndSend(writer);
            }
        }
        
        private struct Event
        {
            public NetworkMessageType messageType;
            public NetworkConnection connection;
            public uint id;
        }

        [BurstCompile]
        private struct Apply : IJobParallelFor
        {
            public NetworkDriver.Concurrent driver;
            
            [ReadOnly]
            public NativeArray<NetworkConnection> connections;

            [ReadOnly] 
            public NativeArray<Command> commands;
            
            [ReadOnly]
            public NativeParallelMultiHashMap<NetworkConnection, int> commandIndices;

            public void Execute(int index)
            {
                var connection = connections[index];
                /*if (NetworkConnection.State.Connected != driver.GetConnectionState(connection))
                    return;*/
                
                if (!commandIndices.TryGetFirstValue(connection, out var commandIndex, out var iterator))
                    return;

                int result;
                do
                {
                    result = commands[commandIndex].Send(ref driver);
                    if(result < 0)
                        __LogError((StatusCode)result);
                    
                } while (commandIndices.TryGetNextValue(out commandIndex, ref iterator));
            }
        }

        [BurstCompile]
        private struct Resize : IJob
        {
            public NetworkDriver driver;
            public NativeBuffer buffer;

            public NativeList<NetworkConnection> connectionsToDisconnect;

            public NativeList<NetworkConnection> connections;

            public NativeList<Event> events;

            public NativeList<Command> commands;

            public NativeList<NetworkServerMessage> messages;

            public NativeParallelMultiHashMap<uint, NetworkServerMessage> buffers;

            public NativeParallelMultiHashMap<NetworkConnection, int> commandIndices;

            public void Execute()
            {
                foreach (var connectionToDisconnect in connectionsToDisconnect)
                    driver.Disconnect(connectionToDisconnect);

                connectionsToDisconnect.Clear();

                NetworkConnection connection;
                while ((connection = driver.Accept()) != default(NetworkConnection))
                    connections.Add(connection);

                int numEvents = 0, numConnections = connections.Length;
                for (int i = 0; i < numConnections; i++)
                    numEvents += math.max(1, driver.GetEventQueueSizeForConnection(connections[i]));

                buffer.Reset();
                buffer.capacity = math.max(buffer.capacity, numEvents * driver.MaxPayloadCapacity());

                events.Clear();
                events.Capacity = math.max(events.Capacity, numEvents);

                messages.Clear();
                messages.Capacity = math.max(messages.Capacity, numEvents);

                buffers.Clear();
                buffers.Capacity = math.max(buffers.Capacity, numEvents);

                commands.Clear();
                commandIndices.Clear();
            }
        }

        [BurstCompile]
        private struct PopEvents : IJobParallelForDefer
        {
            public StreamCompressionModel model;
            public NetworkDriver.Concurrent driver;

            public NativeBuffer.ParallelWriter buffer;

            [ReadOnly]
            public NativeHashMap<NetworkConnection, uint> ids;

            [ReadOnly]
            public NativeArray<NetworkConnection> connections;

            [NativeDisableParallelForRestriction]
            public NativeArray<int> idCount;

            public NativeList<Event>.ParallelWriter events;

            public NativeList<NetworkServerMessage>.ParallelWriter messages;

            public NativeParallelMultiHashMap<uint, NetworkServerMessage>.ParallelWriter buffers;

            public void Execute(int index)
            {
                var connection = connections[index];
                if (NetworkConnection.State.Disconnected == driver.GetConnectionState(connection))
                {
                    __Disconnect(connection, DisconnectReason.Default, 0);

                    return;
                }

                NetworkServerMessage message;
                message.index = 0;
                message.connection = connection;

                uint id;
                DataStreamReader stream;
                NetworkEvent.Type cmd;
                while (true)
                {
                    cmd = driver.PopEventForConnection(connection, out stream);
                    switch (cmd)
                    {
                        case NetworkEvent.Type.Empty:
                            return;
                        case NetworkEvent.Type.Data:
                            message.type = stream.ReadPackedUInt(model);
                            switch (message.type)
                            {
                                case (uint)NetworkMessageType.RPC:
                                    id = stream.ReadPackedUInt(model);

                                    message._block = buffer.WriteBlock(stream.Length - stream.GetBytesRead(), false);

                                    stream.ReadBytes(message._block.AsArray<byte>());

                                    buffers.Add(id, message);
                                    break;
                                case (uint)NetworkMessageType.Register:
                                    if (!ids.TryGetValue(connection, out id))
                                    {
                                        id = (uint)idCount.Increment(0);

                                        Event @event;
                                        @event.messageType = NetworkMessageType.Register;
                                        @event.connection = connection;
                                        @event.id = id;

                                        events.AddNoResize(@event);
                                    }

                                    message._block = buffer.WriteBlock(stream.Length - stream.GetBytesRead(), false);

                                    stream.ReadBytes(message._block.AsArray<byte>());

                                    buffers.Add(id, message);
                                    break;
                                case (uint)NetworkMessageType.Unregister:
                                    if (ids.TryGetValue(connection, out id))
                                    {
                                        Event @event;
                                        @event.messageType = NetworkMessageType.Unregister;
                                        @event.connection = connection;
                                        @event.id = id;

                                        events.AddNoResize(@event);

                                        message._block = buffer.WriteBlock(stream.Length - stream.GetBytesRead(), false);

                                        stream.ReadBytes(message._block.AsArray<byte>());

                                        buffers.Add(id, message);
                                    }
                                    else
                                        UnityEngine.Debug.LogError($"The handle of connection {connection} is missing!");

                                    break;
                                default:
                                    message._block = buffer.WriteBlock(stream.Length - stream.GetBytesRead(), false);

                                    stream.ReadBytes(message._block.AsArray<byte>());

                                    messages.AddNoResize(message);
                                    break;
                            }

                            break;
                        case NetworkEvent.Type.Connect:
                            message.type = (uint)NetworkMessageType.Connect;

                            message._block = default;

                            messages.AddNoResize(message);
                            break;
                        case NetworkEvent.Type.Disconnect:
                            __Disconnect(connection, (DisconnectReason)stream.ReadByte(), message.index);
                            break;
                    }

                    ++message.index;
                }
            }

            private void __Disconnect(in NetworkConnection connection, DisconnectReason disconnectReason, int index)
            {
                NetworkServerMessage message;
                message.index = index;
                message.connection = connection;

                if (ids.TryGetValue(connection, out uint id))
                {
                    Event @event;
                    @event.messageType = NetworkMessageType.Unregister;
                    @event.connection = connection;
                    @event.id = id;

                    events.AddNoResize(@event);

                    message.type = (uint)NetworkMessageType.Unregister;
                    message._block = default;

                    buffers.Add(id, message);
                }
                /*else
                    UnityEngine.Debug.LogError($"The handle of connection {connection} is missing!");*/

                message.type = (uint)NetworkMessageType.Disconnect;

                message._block = buffer.WriteBlock(sizeof(byte), false);

                var writer = message._block.writer;
                writer.Write((byte)disconnectReason);

                messages.AddNoResize(message);
            }
        }

        [BurstCompile]
        private struct DispatchEvents : IJob
        {
            public NetworkDriver driver;

            [ReadOnly]
            public NativeArray<Event> events;

            public NativeArray<NetworkServerMessage> messages;

            public NativeList<NetworkConnection> connections;

            public NativeHashMap<NetworkConnection, uint> ids;

            public void Execute()
            {
                foreach (var @event in events)
                {
                    switch(@event.messageType)
                    {
                        case NetworkMessageType.Register:
                            ids.Add(@event.connection, @event.id);
                            break;
                        case NetworkMessageType.Unregister:
                            ids.Remove(@event.connection);

                            //driver.Disconnect(@event.connection);
                            break;
                    }
                }

                messages.Sort();

                int numConnections = connections.Length;
                for (int i = 0; i < numConnections; ++i)
                {
                    if (NetworkConnection.State.Disconnected == driver.GetConnectionState(connections[i]))
                    {
                        connections.RemoveAtSwapBack(i--);

                        --numConnections;
                    }
                }
            }
        }

        private NetworkDriver __driver;

        private NativeBuffer __buffer;

        private NativeArray<int> __idCount;
        
        private NativeList<NetworkConnection> __connectionsToDisconnects;

        private NativeList<NetworkConnection> __connections;

        private NativeList<Command> __commands;

        private NativeList<Event> __events;

        private NativeList<NetworkServerMessage> __messages;

        private NativeHashMap<NetworkConnection, uint> __ids;

        private NativeParallelMultiHashMap<NetworkConnection, int> __commandIndices;

        private NativeParallelMultiHashMap<uint, NetworkServerMessage> __buffers;

        public bool isCreated => __buffer.isCreated;

        public bool isListening => __driver.Listening;

        public int connectionCount => __connections.Length;

        public NativeHashMap<NetworkConnection, uint> ids
        {
            get => __ids;
        }

        public NativeArray<NetworkServerMessage>.ReadOnly messages => __messages.AsArray().AsReadOnly();

        public NativeParallelMultiHashMap<uint, NetworkServerMessage>.ReadOnly buffers => __buffers.AsReadOnly();

        public NetworkDriver.Concurrent driverConcurrent
        {
            get;
        }

        public NetworkServer(AllocatorManager.AllocatorHandle allocator, in NetworkSettings settings)
        {
            __driver = NetworkDriver.Create(settings);

            __buffer = new NativeBuffer(allocator, 1);

            __idCount = new NativeArray<int>(1, allocator.ToAllocator, NativeArrayOptions.ClearMemory);

            __connectionsToDisconnects = new NativeList<NetworkConnection>(allocator);

            __connections = new NativeList<NetworkConnection>(allocator);

            __commands = new NativeList<Command>(allocator);

            __events = new NativeList<Event>(allocator);

            __messages = new NativeList<NetworkServerMessage>(allocator);

            __ids = new NativeHashMap<NetworkConnection, uint>(1, allocator);

            __commandIndices = new NativeParallelMultiHashMap<NetworkConnection, int>(1, allocator);

            __buffers = new NativeParallelMultiHashMap<uint, NetworkServerMessage>(1, allocator);
            
            driverConcurrent = __driver.ToConcurrent();
        }

        public void Dispose()
        {
            __buffer.Dispose();

            __idCount.Dispose();

            __connectionsToDisconnects.Dispose();

            __connections.Dispose();

            __commands.Dispose();

            __events.Dispose();

            __messages.Dispose();

            __ids.Dispose();

            __commandIndices.Dispose();

            __buffers.Dispose();

            __driver.Dispose();
        }

        public NetworkConnection.State GetConnectionState(in NetworkConnection connection)
        {
            return __driver.GetConnectionState(connection);
        }
        
        public void Disconnect(in NetworkConnection connection)
        {
            __connectionsToDisconnects.Add(connection);
        }

        public void DisconnectAllConnections()
        {
            foreach (var connection in __connections)
                __driver.Disconnect(connection);
        }

        public NetworkPipeline CreatePipeline(NetworkPipelineType type)
        {
            return __driver.CreatePipeline(type);
        }

        public void Listen(ushort port, NetworkFamily family = NetworkFamily.Ipv4)
        {
            NetworkEndpoint endpoint;
            switch(family)
            {
                case NetworkFamily.Ipv4:
                    endpoint = NetworkEndpoint.AnyIpv4;// The local address to which the client will connect to is 127.0.0.1
                    break;
                case NetworkFamily.Ipv6:
                    endpoint = NetworkEndpoint.AnyIpv6;
                    break;
                default:
                    endpoint = default;
                    break;
            }

            endpoint.Port = port;
            if (__driver.Bind(endpoint) != 0 || __driver.Listen() != 0)
                UnityEngine.Debug.LogError($"Failed to bind to port {port}");
        }

        public uint GetID(in NetworkConnection connection)
        {
            if (__ids.TryGetValue(connection, out uint id))
                return id;

            return 0;
        }

        public uint CreateNewID() => (uint)__idCount.Increment(0);

        public StatusCode BeginSendRPC(
            in NetworkPipeline pipeline, 
            in NetworkConnection connection, 
            uint handle, 
            int messageSize, 
            out DataStreamWriter writer)
        {
            var statusCode = BeginSendRPC(pipeline, connection, messageSize + UnsafeUtility.SizeOf<int>(), out writer);
            if (statusCode == StatusCode.Success)
                writer.WritePackedUInt(handle);

            return statusCode;
        }

        public StatusCode BeginSendRPC(in NetworkPipeline pipeline, in NetworkConnection connection, int messageSize, out DataStreamWriter writer)
        {
            if (__ids.TryGetValue(connection, out uint id))
            {
                writer = __Write(messageSize + (UnsafeUtility.SizeOf<int>() << 1), pipeline, connection);

                writer.WritePackedUInt((uint)NetworkMessageType.RPC);
                writer.WritePackedUInt(id);
                writer.Flush();

                return StatusCode.Success;
            }

            writer = default;

            return StatusCode.NetworkIdMismatch;
        }

        public StatusCode BeginSend(
            in NetworkPipeline pipeline, 
            in NetworkConnection connection, 
            uint messageType, 
            int messageSize, 
            out DataStreamWriter writer)
        {
            writer = __Write(
                messageSize + UnsafeUtility.SizeOf<int>() + UnsafeUtility.SizeOf<ushort>(), 
                pipeline, 
                connection);
            
            writer.WritePackedUInt(messageType, StreamCompressionModel.Default);
            writer.WriteUShort(0);
            //writer.Flush();

            return StatusCode.Success;
        }

        public int EndSend(in DataStreamWriter writer)
        {
            var mode = StreamCompressionModel.Default;
            var array = writer.AsNativeArray();
            var reader = new DataStreamReader(array);
            var stream = new DataStreamWriter(array);
            stream.WritePackedUInt(reader.ReadPackedUInt(mode), mode);

            reader.ReadUShort();

            int length = writer.Length, size = length - reader.GetBytesRead();
            UnityEngine.Assertions.Assert.IsFalse(size > ushort.MaxValue);
            stream.WriteUShort((ushort)size);

            int commandIndex = (int)writer.m_SendHandleData - 1;
            ref var command = ref __commands.ElementAt(commandIndex);
            __commandIndices.Add(command.connection, commandIndex);

            command.block = command.block.SubBlock(length);
            
            return size; //driver.EndSend(writer);
        }

        public void GetIDs(ref NativeList<uint> ids)
        {
            uint id;
            var enumerator = __buffers.GetEnumerator();
            while (enumerator.MoveNext())
            {
                id = enumerator.Current.Key;
                if (ids.IndexOf(id) == -1)
                    ids.Add(id);
            }
        }

        public void Receive(uint id, ref NativeList<NetworkServerMessage> messages)
        {
            int index = messages.Length;
            var enumerator = __buffers.GetValuesForKey(id);
            while(enumerator.MoveNext())
                messages.Add(enumerator.Current);

            messages.AsArray().GetSubArray(index, messages.Length - index).Sort();
        }

        public JobHandle ScheduleUpdate(int innerloopBatchCount, in JobHandle inputDeps)
        {
            Apply apply;
            apply.driver = driverConcurrent;
            apply.connections = __connections.AsArray();
            apply.commands = __commands.AsArray();
            apply.commandIndices = __commandIndices;
            var jobHandle = apply.ScheduleByRef(__connections.Length, innerloopBatchCount, inputDeps);

            jobHandle = __driver.ScheduleUpdate(jobHandle);

            Resize resize;
            resize.driver = __driver;
            resize.buffer = __buffer;
            resize.connectionsToDisconnect = __connectionsToDisconnects;
            resize.connections = __connections;
            resize.commands = __commands;
            resize.events = __events;
            resize.messages = __messages;
            resize.buffers = __buffers;
            resize.commandIndices = __commandIndices;
            jobHandle = resize.ScheduleByRef(jobHandle);

            PopEvents popEvents;
            popEvents.model = StreamCompressionModel.Default;
            popEvents.driver = driverConcurrent;
            popEvents.buffer = __buffer.parallelWriter;
            popEvents.idCount = __idCount;
            popEvents.connections = __connections.AsDeferredJobArray();
            popEvents.ids = __ids;
            popEvents.events = __events.AsParallelWriter();
            popEvents.messages = __messages.AsParallelWriter();
            popEvents.buffers = __buffers.AsParallelWriter();
            jobHandle = popEvents.ScheduleByRef(__connections, innerloopBatchCount, jobHandle);

            DispatchEvents dispatchEvents;
            dispatchEvents.driver = __driver;
            dispatchEvents.connections = __connections;
            dispatchEvents.events = __events.AsDeferredJobArray();
            dispatchEvents.messages = __messages.AsDeferredJobArray();
            dispatchEvents.ids = __ids;
            jobHandle = dispatchEvents.ScheduleByRef(jobHandle);

            return jobHandle;
        }

        private DataStreamWriter __Write(
            int messageSize, 
            in NetworkPipeline pipeline, 
            in NetworkConnection connection)
        {
            Command command;
            command.connection = connection;
            command.pipeline = pipeline;
            
            var buffer = __buffer.writer;
            command.block = buffer.WriteBlock(messageSize, false);

            __commands.Add(command);

            var writer = new DataStreamWriter(command.block.AsArray<byte>());
            writer.m_SendHandleData = (IntPtr)__commands.Length;

            return writer;
        }

        private static void __LogError(StatusCode statusCode)
        {
            UnityEngine.Debug.LogError($"Server: {(int)statusCode}");
        }
    }

    public struct NetworkServerManager : IComponentData
    {
        private UnsafeList<LookupJobManager> __lookupJobManager;

        public bool isCreated => __lookupJobManager.IsCreated;

        public ref LookupJobManager lookupJobManager => ref __lookupJobManager.ElementAt(0);

        public NetworkServer server
        {
            get;
        }

        public static EntityQuery GetEntityQuery(ref SystemState state)
        {
            using (var builder = new EntityQueryBuilder(Allocator.Temp))
                return builder
                    .WithAll<NetworkServerManager>()
                    .WithOptions(EntityQueryOptions.IncludeSystems)
                    .Build(ref state);
        }

        public NetworkServerManager(AllocatorManager.AllocatorHandle allocator, in NetworkSettings settings)
        {
            __lookupJobManager = new UnsafeList<LookupJobManager>(1, allocator, NativeArrayOptions.UninitializedMemory);
            __lookupJobManager.Resize(1, NativeArrayOptions.ClearMemory);

            server = new NetworkServer(allocator, settings);
        }

        public void Dispose()
        {
            lookupJobManager.CompleteReadWriteDependency();

            __lookupJobManager.Dispose();

            server.Dispose();
        }

        public JobHandle Update(int innerloopBatchCount, in JobHandle inputDeps)
        {
            var jobHandle = JobHandle.CombineDependencies(lookupJobManager.readWriteJobHandle, inputDeps);
            jobHandle = server.ScheduleUpdate(innerloopBatchCount, jobHandle);

            lookupJobManager.readWriteJobHandle = jobHandle;

            return jobHandle;
        }
    }

    [BurstCompile]
    public partial struct NetworkServerSystem : ISystem
    {
        public static readonly int InnerloopBatchCount = 4;

        public NetworkServerManager manager
        {
            get;

            private set;
        }

        public static NetworkServerManager CreateManager(in WorldUnmanaged world, in NetworkSettings settings)
        {
            var manager = new NetworkServerManager(Allocator.Persistent, settings);

            var systemHandle = world.GetExistingUnmanagedSystem<NetworkServerSystem>();

            ref var system = ref world.GetUnsafeSystemRef<NetworkServerSystem>(systemHandle);
            if (system.manager.isCreated)
            {
                system.manager.Dispose();

                world.EntityManager.SetComponentData(systemHandle, manager);
            }
            else
                world.EntityManager.AddComponentData(systemHandle, manager);

            system.manager = manager;

            return manager;
        }

        [BurstCompile]
        public void OnCreate(ref SystemState state)
        {
        }

        [BurstCompile]
        public void OnDestroy(ref SystemState state)
        {
            if (manager.isCreated)
                manager.Dispose();
        }

        [BurstCompile]
        public void OnUpdate(ref SystemState state)
        {
            var manager = this.manager;
            if (!manager.isCreated)
                return;

            state.Dependency = manager.Update(InnerloopBatchCount, state.Dependency);
        }
    }
}