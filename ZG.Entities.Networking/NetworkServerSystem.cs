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
    public interface INetworkDriver
    {
        NetworkConnection.State GetConnectionState(in NetworkConnection connection);

        StatusCode BeginSend(in NetworkPipeline pipe, in NetworkConnection id, out DataStreamWriter writer, int requiredPayloadSize = 0);

        int EndSend(DataStreamWriter writer);

        void AbortSend(DataStreamWriter writer);
    }

    public struct NetworkDriverWrapper : INetworkDriver
    {
        private struct Command
        {
#if DEBUG
            public bool isActive;
#endif
            public NetworkBufferSegment bufferSegment;
            public NetworkPipeline pipeline;
            public IntPtr ptr;
        }

        private NetworkDriver.Concurrent __instance;
        private NativeArray<NetworkBuffer> __bufferPool;
        private NativeArray<int> __bufferPoolCount;
        private UnsafeList<Command> __commands;
        private UnsafeHashMap<NetworkPipeline, NetworkBuffer> __buffers;

        public NetworkDriverWrapper(
            ref NetworkDriver.Concurrent instance, 
            ref NativeArray<int> bufferPoolCount,
            in NativeArray<NetworkBuffer> bufferPool)
        {
            __instance = instance;
            __bufferPool = bufferPool;
            __bufferPoolCount = bufferPoolCount;
            __commands = default;
            __buffers = default;
        }

        public void Dispose()
        {
            if (__commands.IsCreated)
                __commands.Dispose();

            if (__buffers.IsCreated)
                __buffers.Dispose();
        }

        public void Apply<T>(in T id,
            ref NativeParallelMultiHashMap<T, NetworkBufferInstance>.ParallelWriter bufferInstances) where T : unmanaged, IEquatable<T>
        {
            if (__buffers.IsCreated)
            {
                NetworkBufferInstance bufferInstance;
                foreach (var buffer in __buffers)
                {
                    bufferInstance.pipeline = buffer.Key;
                    bufferInstance.buffer = buffer.Value;
                    bufferInstances.Add(id, bufferInstance);
                }

#if DEBUG
                foreach (var command in __commands)
                    UnityEngine.Assertions.Assert.IsFalse(command.isActive);
#endif
            }
        }

        public NetworkBuffer GetOrCreate(in NetworkPipeline pipeline, int capacity)
        {
            if (!__buffers.IsCreated)
                __buffers = new UnsafeHashMap<NetworkPipeline, NetworkBuffer>(1, Allocator.Temp);

            if (!__buffers.TryGetValue(pipeline, out var buffer))
            {
                int bufferIndex = __bufferPoolCount.Decrement(0);
                if (bufferIndex < 0)
                {
                    __bufferPoolCount.Increment(0);

                    buffer = new NetworkBuffer(capacity, Allocator.Persistent);
                }
                else
                {
                    buffer = __bufferPool[bufferIndex];

                    buffer.Clear();
                }
            }

            return buffer;
        }

        public NetworkConnection.State GetConnectionState(in NetworkConnection connection)
        {
            return __instance.GetConnectionState(connection);
        }

        public StatusCode BeginSend(
            in NetworkPipeline pipeline,
            in NetworkConnection connection,
            out DataStreamWriter writer,
            int requiredPayloadSize = 0)
        {
            Command command;
#if DEBUG
            command.isActive = true;
#endif
            command.pipeline = pipeline;

            if (!__buffers.IsCreated)
            {
                var statusCode = (StatusCode)__instance.BeginSend(pipeline, connection, out writer, requiredPayloadSize);
                switch (statusCode)
                {
                    case StatusCode.Success:
                        command.bufferSegment = default;
                        command.ptr = writer.GetSendHandleData();

                        if (!__commands.IsCreated)
                            __commands = new UnsafeList<Command>(1, Allocator.Temp);

                        __commands.Add(command);

                        writer.SetSendHandleData((IntPtr)__commands.Length);

                        return StatusCode.Success;
                    case StatusCode.NetworkSendQueueFull:
                        break;
                    default:
                        return statusCode;
                }
            }

            command.bufferSegment.length = __instance.PayloadCapacity(pipeline);

            if (requiredPayloadSize > command.bufferSegment.length)
            {
                writer = default;

                return StatusCode.NetworkPacketOverflow;
            }
            else if (requiredPayloadSize > 0)
                command.bufferSegment.length = requiredPayloadSize;

            var buffer = GetOrCreate(pipeline, command.bufferSegment.length);

            command.bufferSegment.byteOffset = buffer.value.length;

            command.ptr = IntPtr.Zero;

            if (!__commands.IsCreated)
                __commands = new UnsafeList<Command>(1, Allocator.Temp);

            __commands.Add(command);

            writer = DataStreamUtility.CreateWriter(
                buffer.value.writer.WriteBlock(command.bufferSegment.length, false).AsArray<byte>(),
                (IntPtr)__commands.Length);

            __buffers[pipeline] = buffer;

            return StatusCode.Success;
        }

        public int EndSend(DataStreamWriter writer)
        {
            int length = writer.Length,
                commandIndex = (int)writer.GetSendHandleData() - 1;
            var command = __commands[commandIndex];
#if DEBUG
            if (length > NetworkParameterConstants.MTU)
                UnityEngine.Debug.LogError("Long Stream.");

            UnityEngine.Assertions.Assert.IsTrue(command.isActive);

            command.isActive = false;

            __commands[commandIndex] = command;
#endif

            NetworkBuffer buffer;
            if (command.ptr == IntPtr.Zero)
            {
#if DEBUG
                writer.SetSendHandleData(IntPtr.Zero);
#endif

                if (!__buffers.TryGetValue(command.pipeline, out buffer))
                    return (int)StatusCode.NetworkSendHandleInvalid;

                int position = command.bufferSegment.byteOffset + length;
                if (buffer.value.length == command.bufferSegment.byteOffset + command.bufferSegment.length)
                    buffer.value.length = position;
            }
            else
            {
                writer.SetSendHandleData(command.ptr);

                int result = __instance.EndSend(writer);

#if DEBUG
                writer.SetSendHandleData(IntPtr.Zero);
#endif

                if (result >= 0)
                    return result;

                if (StatusCode.NetworkSendQueueFull != (StatusCode)result)
                    return result;

                buffer = GetOrCreate(command.pipeline, length);

                command.bufferSegment.byteOffset = buffer.value.position;

                buffer.value.writer.WriteBlock(length, false).AsArray<byte>().CopyFrom(writer.AsNativeArray());
            }

            command.bufferSegment.length = length;
            buffer.segments.Add(command.bufferSegment);

            __buffers[command.pipeline] = buffer;

            return length;
        }

        public void AbortSend(DataStreamWriter writer)
        {
            int length = writer.Length,
                commandIndex = (int)writer.GetSendHandleData() - 1;
            var command = __commands[commandIndex];
            NetworkBuffer buffer;
            if (command.ptr == IntPtr.Zero)
            {
                if (!__buffers.TryGetValue(command.pipeline, out buffer))
                    return;

                int position = command.bufferSegment.byteOffset + length;
                if (buffer.value.length == command.bufferSegment.byteOffset + command.bufferSegment.length)
                {
                    buffer.value.length = position;

                    __buffers[command.pipeline] = buffer;
                }
            }
            else
            {
                writer.SetSendHandleData(command.ptr);

                __instance.AbortSend(writer);
            }
        }
    }

    public struct NetworkBufferSegment
    {
        public int byteOffset;
        public int length;

        public NativeArray<TValue> GetArray<TValue, TBuffer>(in TBuffer buffer) 
            where TValue : struct
            where TBuffer : IUnsafeBuffer
        {
            return length < 0 ? default : buffer.AsArray<TValue>(byteOffset, length);
        }

        public NativeArray<byte> GetArray<T>(in T buffer) where T : IUnsafeBuffer
        {
            return GetArray<byte, T>(buffer);
        }
    }

    public struct NetworkBuffer
    {
        public UnsafeList<NetworkBufferSegment> segments;
        public UnsafeBuffer value;

        public NetworkBuffer(int initialCapacity, AllocatorManager.AllocatorHandle allocator)
        {
            segments = new UnsafeList<NetworkBufferSegment>(1, allocator, NativeArrayOptions.UninitializedMemory);
            value = new UnsafeBuffer(initialCapacity, 1, allocator);
        }

        public void Dispose()
        {
            segments.Dispose();
            value.Dispose();
        }

        public void Clear()
        {
            segments.Clear();
            value.Reset();
        }

        public void Apply<T>(
            in NetworkConnection connection,
            in NetworkPipeline pipeline,
            ref T driver,
            ref DataStreamWriter writer) where T : INetworkDriver
        {
            int numSegments = segments.Length, result;
            StatusCode statusCode;
            bool isSend;
            for (int i = 0; i < numSegments; ++i)
            {
                if (writer.IsCreated)
                    isSend = false;
                else
                {
                    statusCode = driver.BeginSend(pipeline, connection, out writer);
                    if (StatusCode.Success != statusCode)
                    {
                        LogError(statusCode);

                        writer = default;

                        break;
                    }

                    isSend = true;
                }

                ref readonly var segment = ref segments.ElementAt(i);
                if (segment.length > writer.Capacity - writer.Length)
                {
                    if (isSend)
                    {
                        driver.AbortSend(writer);

                        writer = default;

                        LogError(StatusCode.NetworkPacketOverflow);

                        break;
                    }

                    result = driver.EndSend(writer);
                    if (result < 0)
                        LogError((StatusCode)result);

                    writer = default;

                    --i;
                }
                else
                    writer.WriteBytes(segment.GetArray(value));
            }
        }

        public static void LogError(StatusCode statusCode)
        {
            UnityEngine.Debug.LogError($"NetworkBuffer: {(int)statusCode}");
        }
    }

    public struct NetworkBufferInstance
    {
        public NetworkPipeline pipeline;
        public NetworkBuffer buffer;
    }

    public struct NetworkDriverBuffer<T> where T : unmanaged, IEquatable<T>
    {
        public struct Command
        {
#if DEBUG
            public bool isActive;
#endif
            public T connection;
            public NetworkPipeline pipeline;
            public NetworkBufferSegment bufferSegment;
        }

        private NetworkDriver.Concurrent __instance;
        private NativeList<NetworkBuffer> __bufferPool;
        private NativeList<Command> __commands;
        private NativeParallelMultiHashMap<T, NetworkBufferInstance> __bufferInstances;

        public NetworkDriverBuffer(
            ref NetworkDriver.Concurrent instance,
            ref NativeList<NetworkBuffer> bufferPool,
            ref NativeList<Command> commands,
            ref NativeParallelMultiHashMap<T, NetworkBufferInstance> bufferInstances)
        {
            __instance = instance;
            __bufferPool = bufferPool;
            __commands = commands;
            __bufferInstances = bufferInstances;
        }

        public bool TryGetBuffer(
            in T connection,
            in NetworkPipeline pipeline,
            out NetworkBufferInstance bufferInstance,
            out NativeParallelMultiHashMapIterator<T> iterator)
        {
            if (__bufferInstances.TryGetFirstValue(connection, out bufferInstance, out iterator))
            {
                do
                {
                    if (bufferInstance.pipeline == pipeline)
                    {
                        return true;
                    }

                } while (__bufferInstances.TryGetNextValue(out bufferInstance, ref iterator));
            }

            return false;
        }

        public NetworkConnection.State GetConnectionState(in NetworkConnection connection)
        {
            return __instance.GetConnectionState(connection);
        }

        public StatusCode BeginSend(
            in NetworkPipeline pipeline,
            in T connection,
            out DataStreamWriter writer,
            int requiredPayloadSize = 0)
        {
            Command command;
            command.bufferSegment.length = __instance.PayloadCapacity(pipeline);
            if (requiredPayloadSize > 0)
            {
                if (requiredPayloadSize > command.bufferSegment.length)
                {
                    writer = default;

                    return StatusCode.NetworkPacketOverflow;
                }

                command.bufferSegment.length = requiredPayloadSize;
            }

            if (TryGetBuffer(connection, pipeline, out var bufferInstance, out var iterator))
                __bufferInstances.Remove(iterator);
            else
            {
                bufferInstance.pipeline = pipeline;

                int bufferPoolLength = __bufferPool.Length;
                if (bufferPoolLength > 0)
                {
                    int bufferIndex = bufferPoolLength - 1;

                    bufferInstance.buffer = __bufferPool[bufferIndex];
                    bufferInstance.buffer.Clear();

                    __bufferPool.ResizeUninitialized(bufferIndex);
                }
                else
                    bufferInstance.buffer = new NetworkBuffer(command.bufferSegment.length, Allocator.Persistent);
            }

            command.bufferSegment.byteOffset = bufferInstance.buffer.value.length;
            command.pipeline = pipeline;
            command.connection = connection;

#if DEBUG
            command.isActive = true;
#endif

            __commands.Add(command);

            writer = DataStreamUtility.CreateWriter(
                bufferInstance.buffer.value.writer.WriteBlock(command.bufferSegment.length, false).AsArray<byte>(),
                (IntPtr)__commands.Length);

            __bufferInstances.Add(connection, bufferInstance);

            return StatusCode.Success;
        }

        public int EndSend(DataStreamWriter writer)
        {
            int commandIndex = (int)writer.GetSendHandleData() - 1;
            var command = __commands[commandIndex];

#if DEBUG
            UnityEngine.Assertions.Assert.IsTrue(command.isActive);

            command.isActive = false;
            __commands[commandIndex] = command;

            writer.SetSendHandleData(IntPtr.Zero);
#endif

            if (!TryGetBuffer(command.connection, command.pipeline, out var bufferInstance, out var iterator))
                return (int)StatusCode.NetworkSendHandleInvalid;

            int length = writer.Length;

#if DEBUG
            if (length > NetworkParameterConstants.MTU)
                UnityEngine.Debug.LogError("Long Stream.");
#endif

            UnityEngine.Assertions.Assert.AreEqual(bufferInstance.buffer.value.length,
                command.bufferSegment.byteOffset + command.bufferSegment.length);

            bufferInstance.buffer.value.length = command.bufferSegment.byteOffset + length;

            command.bufferSegment.length = length;

            bufferInstance.buffer.segments.Add(command.bufferSegment);

            __bufferInstances.SetValue(bufferInstance, iterator);

            return length;
        }

        public void AbortSend(DataStreamWriter writer)
        {
            int commandIndex = (int)writer.GetSendHandleData() - 1;
            var command = __commands[commandIndex];

#if DEBUG
            command.isActive = false;
            __commands[commandIndex] = command;

            writer.SetSendHandleData(IntPtr.Zero);
#endif

            if (!TryGetBuffer(command.connection, command.pipeline, out var bufferInstance, out var iterator))
                return;

            UnityEngine.Assertions.Assert.AreEqual(bufferInstance.buffer.value.length,
                command.bufferSegment.byteOffset + command.bufferSegment.length);

            bufferInstance.buffer.value.length = command.bufferSegment.byteOffset;

            __bufferInstances.SetValue(bufferInstance, iterator);
        }
    }

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
            public NativeArray<NetworkBuffer> bufferPool;

            [ReadOnly]
            public NativeParallelMultiHashMap<NetworkConnection, NetworkBufferInstance> bufferInstanceInputs;
            
            public NativeParallelMultiHashMap<NetworkConnection, NetworkBufferInstance>.ParallelWriter bufferInstanceOutputs;

            [NativeDisableParallelForRestriction]
            public NativeArray<int> bufferPoolCount;

            public void Execute(int index)
            {
                var connection = connections[index];
                /*if (NetworkConnection.State.Connected != driver.GetConnectionState(connection))
                    return;*/
                
                if (!bufferInstanceInputs.TryGetFirstValue(connection, out var bufferInstance, out var iterator))
                    return;

                var driver = new NetworkDriverWrapper(ref this.driver, ref bufferPoolCount, bufferPool);

                DataStreamWriter writer = default;
                do
                {
                    bufferInstance.buffer.Apply(connection, bufferInstance.pipeline, ref driver, ref writer);

                    if (writer.IsCreated)
                    {
                        int result = driver.EndSend(writer);
                        if (result < 0)
                            NetworkBuffer.LogError((StatusCode)result);

                        writer = default;
                    }
                } while (bufferInstanceInputs.TryGetNextValue(out bufferInstance, ref iterator));
                
                driver.Apply(connection, ref bufferInstanceOutputs);
                
                driver.Dispose();
            }
        }

        [BurstCompile]
        private struct Resize : IJob
        {
            public NetworkDriver driver;
            public NativeBuffer buffer;

            [ReadOnly]
            public NativeArray<int> bufferPoolCount;

            public NativeList<NetworkBuffer> bufferPool;

            public NativeList<NetworkConnection> connectionsToDisconnect;

            public NativeList<NetworkConnection> connections;

            public NativeList<Event> events;

            public NativeList<NetworkServerMessage> messages;

            public NativeParallelMultiHashMap<uint, NetworkServerMessage> buffers;

            public NativeParallelMultiHashMap<NetworkConnection, NetworkBufferInstance> bufferInstanceInputs;
            
            public NativeParallelMultiHashMap<NetworkConnection, NetworkBufferInstance> bufferInstanceOutputs;

            public void Execute()
            {
                bufferPool.ResizeUninitialized(bufferPoolCount[0]);
                
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

                bufferInstanceInputs.Clear();
                
                foreach (var bufferInstance in bufferInstanceOutputs)
                    bufferInstanceInputs.Add(bufferInstance.Key, bufferInstance.Value);

                bufferInstanceOutputs.Clear();
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
        
        private NativeArray<int> __bufferPoolCount;

        private NativeList<NetworkBuffer> __bufferPool;

        private NativeList<NetworkConnection> __connectionsToDisconnects;

        private NativeList<NetworkConnection> __connections;

        private NativeList<NetworkDriverBuffer<NetworkConnection>.Command> __commands;

        private NativeList<Event> __events;

        private NativeList<NetworkServerMessage> __messages;

        private NativeHashMap<NetworkConnection, uint> __ids;

        private NativeParallelMultiHashMap<uint, NetworkServerMessage> __buffers;

        private NativeParallelMultiHashMap<NetworkConnection, NetworkBufferInstance> __bufferInstanceInputs;
            
        private NativeParallelMultiHashMap<NetworkConnection, NetworkBufferInstance> __bufferInstanceOutputs;

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

        public NetworkDriverBuffer<NetworkConnection> driver
        {
            get
            {
                var instance = driverConcurrent;
                return new NetworkDriverBuffer<NetworkConnection>(ref instance, ref __bufferPool, ref __commands, ref __bufferInstanceInputs);
            }
        }

        public NetworkServer(AllocatorManager.AllocatorHandle allocator, in NetworkSettings settings)
        {
            __driver = NetworkDriver.Create(settings);

            __buffer = new NativeBuffer(allocator, 1);

            __idCount = new NativeArray<int>(1, allocator.ToAllocator, NativeArrayOptions.ClearMemory);

            __bufferPoolCount = new NativeArray<int>(1, Allocator.Persistent, NativeArrayOptions.ClearMemory);

            __bufferPool = new NativeList<NetworkBuffer>(Allocator.Persistent);

            __connectionsToDisconnects = new NativeList<NetworkConnection>(allocator);

            __connections = new NativeList<NetworkConnection>(allocator);

            __commands = new NativeList<NetworkDriverBuffer<NetworkConnection>.Command>(allocator);

            __events = new NativeList<Event>(allocator);

            __messages = new NativeList<NetworkServerMessage>(allocator);

            __ids = new NativeHashMap<NetworkConnection, uint>(1, allocator);

            __buffers = new NativeParallelMultiHashMap<uint, NetworkServerMessage>(1, allocator);
            
            __bufferInstanceInputs = new NativeParallelMultiHashMap<NetworkConnection, NetworkBufferInstance>(1, Allocator.Persistent);
            
            __bufferInstanceOutputs = new NativeParallelMultiHashMap<NetworkConnection, NetworkBufferInstance>(1, Allocator.Persistent);

            driverConcurrent = __driver.ToConcurrent();
        }

        public void Dispose()
        {
            __driver.Dispose();
            
            __buffer.Dispose();

            __idCount.Dispose();

            __bufferPoolCount.Dispose();

            foreach (var buffer in __bufferPool)
                buffer.value.Dispose();
            
            __bufferPool.Dispose();

            __connectionsToDisconnects.Dispose();

            __connections.Dispose();

            __commands.Dispose();

            __events.Dispose();

            __messages.Dispose();

            __ids.Dispose();

            __buffers.Dispose();

            foreach (var bufferInstance in __bufferInstanceInputs)
                bufferInstance.Value.buffer.Dispose();

            __bufferInstanceInputs.Dispose();
            
            foreach (var bufferInstance in __bufferInstanceOutputs)
                bufferInstance.Value.buffer.Dispose();

            __bufferInstanceOutputs.Dispose();
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
            out DataStreamWriter writer, 
            int requiredPayloadSize = 0)
        {
            var statusCode = BeginSendRPC(
                pipeline, 
                connection, 
                out writer, 
                requiredPayloadSize == 0 ? 0 : requiredPayloadSize + UnsafeUtility.SizeOf<int>());
            if (statusCode == StatusCode.Success)
                writer.WritePackedUInt(handle);

            return statusCode;
        }

        public StatusCode BeginSendRPC(
            in NetworkPipeline pipeline, 
            in NetworkConnection connection, 
            out DataStreamWriter writer, 
            int requiredPayloadSize = 0)
        {
            if (__ids.TryGetValue(connection, out uint id))
            {
                var statusCode = driver.BeginSend(
                    pipeline, 
                    connection, 
                    out writer, 
                    requiredPayloadSize == 0 ? 0 : requiredPayloadSize + (UnsafeUtility.SizeOf<int>() << 1));
                if (StatusCode.Success == statusCode)
                {
                    var model = StreamCompressionModel.Default;
                    writer.WritePackedUInt((uint)NetworkMessageType.RPC, model);
                    writer.WritePackedUInt(id, model);
                    writer.Flush();
                }

                return statusCode;
            }

            writer = default;

            return StatusCode.NetworkIdMismatch;
        }

        public StatusCode BeginSend(
            in NetworkPipeline pipeline, 
            in NetworkConnection connection, 
            uint messageType, 
            out DataStreamWriter writer, 
            int requiredPayloadSize = 0)
        {
            var statusCode = driver.BeginSend(
                pipeline, 
                connection, 
                out writer, 
                requiredPayloadSize == 0 ? 0 : requiredPayloadSize + UnsafeUtility.SizeOf<int>() + UnsafeUtility.SizeOf<ushort>());
            if (StatusCode.Success == statusCode)
            {
                writer.WritePackedUInt(messageType, StreamCompressionModel.Default);
                writer.WriteUShort(0);
                //writer.Flush();
            }

            return statusCode;
        }

        public int EndSend(in DataStreamWriter writer)
        {
            var mode = StreamCompressionModel.Default;
            var array = writer.AsNativeArray();
            var reader = new DataStreamReader(array);
            uint messageType = reader.ReadPackedUInt(mode);
            switch (messageType)
            {
                case (uint)NetworkMessageType.RPC:
                case (uint)NetworkMessageType.Register:
                case (uint)NetworkMessageType.Unregister:
                    break;
                default:
                    var stream = new DataStreamWriter(array);
                    stream.WritePackedUInt(messageType, mode);

                    reader.ReadUShort();

                    int length = writer.Length, size = length - reader.GetBytesRead();
                    UnityEngine.Assertions.Assert.IsFalse(size > ushort.MaxValue);
                    stream.WriteUShort((ushort)size);

                    break;
            }

            return driver.EndSend(writer);
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
            __commands.Clear();
            
            __bufferPoolCount[0] = __bufferPool.Length;
            
            Apply apply;
            apply.driver = driverConcurrent;
            apply.connections = __connections.AsArray();
            apply.bufferPool = __bufferPool.AsArray();
            apply.bufferInstanceInputs = __bufferInstanceInputs;
            apply.bufferInstanceOutputs = __bufferInstanceOutputs.AsParallelWriter();
            apply.bufferPoolCount = __bufferPoolCount;
            var jobHandle = apply.ScheduleByRef(__connections.Length, innerloopBatchCount, inputDeps);

            jobHandle = __driver.ScheduleUpdate(jobHandle);

            Resize resize;
            resize.driver = __driver;
            resize.buffer = __buffer;
            resize.bufferPoolCount = __bufferPoolCount;
            resize.bufferPool = __bufferPool;
            resize.connectionsToDisconnect = __connectionsToDisconnects;
            resize.connections = __connections;
            resize.events = __events;
            resize.messages = __messages;
            resize.buffers = __buffers;
            resize.bufferInstanceInputs = __bufferInstanceInputs;
            resize.bufferInstanceOutputs = __bufferInstanceOutputs;
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