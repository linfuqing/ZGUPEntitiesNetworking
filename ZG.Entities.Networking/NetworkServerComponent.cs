using System;
using System.Collections;
using System.Collections.Generic;
using Unity.Entities;
using Unity.Collections;
using Unity.Mathematics;
using Unity.Networking.Transport;
using Unity.Networking.Transport.Error;
using UnityEngine;
using NetworkReader = Unity.Collections.DataStreamReader;
using NetworkWriter = Unity.Collections.DataStreamWriter;
using static Unity.Networking.Transport.NetworkParameterConstants;

namespace ZG
{
    public interface INetworkServerWrapper
    {
        int GetLayer(int type);

        bool Register(
            NetworkReader reader,
            in NetworkConnection connection,
            ref uint id,
            out int node,
            out int layerMask,
            out float visibilityDistance);

        bool Unregister(uint id, in NetworkConnection connection, NetworkReader reader);

        bool Init(bool isNew, uint sourceID, uint destinationID, ref NetworkWriter writer);

        void SendRegisterMessage(ref NetworkWriter writer, uint id);

        void SendUnregisterMessage(ref NetworkWriter writer, uint id);
    }

    public delegate void NetworkServerHandler(NetworkConnection connection, NetworkReader reader);

    public interface INetworkServer
    {
        public event Action<NetworkConnection> onConnect;
        public event Action<NetworkConnection, DisconnectReason> onDisconnect;

        public event Action<uint, bool> onActive;

        bool isListening { get; }

        bool isExclusivingTransaction { get; }

        int maxConnectionCount { get; set; }

        int connectionCount { get; }

        bool IsConnected(in NetworkConnection connection);

        void Disconnect(in NetworkConnection connection);

        uint GetID(in NetworkConnection connection);

        NetworkConnection GetConnection(uint id);

        NetworkIdentityComponent GetIdentity(uint id, bool isRegisteredOnly = true);

        bool Register(uint id, NetworkReader reader);

        bool Unregister(uint id, NetworkReader reader);

        bool Move(uint id, int node, int layerMask, float visibilityDistance);

        bool Send<T>(int channel, uint messageType, uint id, T message) where T : INetworkMessage;

        bool Send<T>(int channel, uint messageType, in NetworkConnection connection, T message) where T : INetworkMessage;

        bool Send(int channel, uint messageType, in NetworkConnection connection);

        bool BeginSend(int channel, uint messageType, in NetworkConnection connection, out NetworkWriter writer);

        int EndSend(NetworkWriter writer);

        void Configure(NativeArray<NetworkPipelineType> pipelineTypes);

        bool Listen(ushort port, NetworkFamily family = NetworkFamily.Ipv4);

        //void Shutdown();

        bool IsExclusiveTransaction(uint id);

        void ExclusiveTransactionStart();

        void ExclusiveTransactionEnd();

        void UnregisterHandler(uint messageType);

        void RegisterHandler(uint messageType, NetworkServerHandler handler);

        uint CreateNewID();

        NetworkIdentityComponent Instantiate(
            NetworkIdentityComponent prefab,
            in Quaternion rotation,
            in Vector3 position,
            int type,
            uint id, 
            in NetworkConnection connection = default);

        void Destroy(uint id);

        bool TryGetNode(uint id, out int node);

        bool NodeContains(int node, uint id);
    }

    public interface INetworkHostServer
    {
        int EndRPC(NetworkRPCType type, NetworkWriter writer, in NativeArray<uint> additionalIDs = default, in NativeArray<uint> maskIDs = default);

        int EndRPC(NetworkWriter writer, in NativeArray<uint> specifiedIDs);

        int EndRPCWithout(NetworkWriter writer, in NativeArray<uint> ids);
    }

    public sealed class NetworkServerComponent : MonoBehaviour, INetworkHost, INetworkServer, INetworkHostServer
    {
        //public int maxConnectionCount = 64;

        [SerializeField]
        internal int _connectTimeoutMS = ConnectTimeoutMS;
        [SerializeField]
        internal int _maxConnectAttempts = MaxConnectAttempts;
        [SerializeField]
        internal int _disconnectTimeoutMS = DisconnectTimeoutMS;
        [SerializeField]
        internal int _heartbeatTimeoutMS = HeartbeatTimeoutMS;
        [SerializeField]
        internal int _reconnectionTimeoutMS = ReconnectionTimeoutMS;
        [SerializeField]
        internal int _maxFrameTimeMS = 0;
        [SerializeField]
        internal int _fixedFrameTimeMS = 0;
        [SerializeField]
        internal int _receiveQueueCapacity = 4096;//ReceiveQueueCapacity;
        [SerializeField]
        internal int _sendQueueCapacity = 4096;// SendQueueCapacity;

        [SerializeField]
        internal int _defaultChannel;

        [SerializeField]
        internal string _worldName = "Server";

        private World __world;
        private NetworkServerManager __manager;
        private NetworkRPCController __controller;
        private INetworkServerWrapper __wrapper;

        private NativeArray<NetworkPipeline> __pipelines;
        private NativeList<uint> __ids;
        private NativeList<NetworkServerMessage> __messages;

        private Dictionary<uint, NetworkServerHandler> __handlers;
        private Dictionary<uint, NetworkIdentityComponent> __identities;

        private Dictionary<int, HashSet<uint>> __freeIdentityIDs;

        private HashSet<uint> __exclusiveTransactionIdentityIDs;

        public event Action<NetworkConnection> onConnect;
        public event Action<NetworkConnection, DisconnectReason> onDisconnect;

        public event Action<uint, bool> onActive;

        public bool isConfigured => __pipelines.IsCreated;

        public bool isListening => server.isListening;

        public bool isExclusivingTransaction
        {
            get;

            private set;
        }

        public int maxConnectionCount
        {
            get => _maxConnectAttempts;

            set
            {
                if (isConfigured)
                    throw new InvalidOperationException();

                _maxConnectAttempts = value;
            }
        }

        public int connectionCount => server.connectionCount;

        public World world
        {
            get
            {
                if(__world == null)
                    __world = WorldUtility.GetWorld(_worldName);

                return __world;
            }
        }

        public NetworkServer server
        {
            get
            {
                if (!__manager.isCreated)
                {
                    var settings = new NetworkSettings(Allocator.Temp);

                    settings.WithNetworkConfigParameters(
                        _connectTimeoutMS,
                        _maxConnectAttempts,
                        _disconnectTimeoutMS,
                        _heartbeatTimeoutMS,
                        _reconnectionTimeoutMS,
                        _maxFrameTimeMS,
                        _fixedFrameTimeMS,
                        _receiveQueueCapacity,
                        _sendQueueCapacity);

                    __manager = NetworkServerSystem.CreateManager(world.Unmanaged, settings);
                }

                __manager.lookupJobManager.CompleteReadWriteDependency();

                return __manager.server;
            }
        }

        public NativeGraphEx<int> graph
        {

            get
            {
                var controller = __GetController();

                controller.lookupJobManager.CompleteReadWriteDependency();

                return controller.graph;
            }
        }

        public NetworkRPCManager<int> rpcManager
        {
            get
            {
                var controller = __GetController();

                controller.lookupJobManager.CompleteReadWriteDependency();

                return controller.manager;
            }
        }

        public NetworkRPCCommander commander
        {
            get
            {
                var controller = __GetController();

                controller.lookupJobManager.CompleteReadWriteDependency();

                return controller.commander;
            }
        }

        public ref NetworkEntityManager entityManager
        {
            get
            {
                //var controller = __GetController();

                //controller.lookupJobManager.CompleteReadWriteDependency();

                return ref world.GetOrCreateSystemUnmanaged<NetworkEntityManager>();
            }
        }

        public bool IsExclusiveTransaction(uint id) => __exclusiveTransactionIdentityIDs != null && __exclusiveTransactionIdentityIDs.Contains(id);

        public bool IsPlayer(uint id)
        {
            return rpcManager.GetConnection(id).IsCreated;
        }

        public bool IsConnected(in NetworkConnection connection)
        {
            return NetworkConnection.State.Connected == server.driver.GetConnectionState(connection);
        }

        public uint GetID(in NetworkConnection connection)
        {
            return server.GetID(connection);
        }

        public NetworkConnection GetConnection(uint id)
        {
            return rpcManager.GetConnection(id);
        }

        public NetworkIdentityComponent GetIdentity(uint id, bool isRegisteredOnly = true)
        {
            return __identities != null && __identities.TryGetValue(id, out var value) && (!isRegisteredOnly || value._host == (INetworkHost)this) ? value : null;
        }

        public uint CreateNewID()
        {
            return server.CreateNewID();
        }

        public NetworkIdentityComponent Instantiate(
            NetworkIdentityComponent prefab, 
            in Quaternion rotation, 
            in Vector3 position, 
            int type,
            uint id,
            in NetworkConnection connection = default)
        {
            if (__freeIdentityIDs != null && __freeIdentityIDs.TryGetValue(type, out var freeIdentityIDs))
            {
                ref var manager = ref this.entityManager;
                //var entityManager = world.EntityManager;
                HashSet<uint>.Enumerator enumerator;
                uint freeIdentityID;
                bool isContinue;
                do
                {
                    isContinue = false;

                    enumerator = freeIdentityIDs.GetEnumerator();
                    while (enumerator.MoveNext())
                    {
                        freeIdentityID = enumerator.Current;

                        Entity entity = manager.GetEntity(freeIdentityID);
                        if (entity == Entity.Null/* && entityManager.HasComponent<NetworkIdentity>(entity)*/)
                        {
                            manager.Unregister(freeIdentityID, false);

                            freeIdentityIDs.Remove(freeIdentityID);

                            isContinue = true;

                            break;
                        }
                        else
                        {
                            if (manager.Change(freeIdentityID, id))
                            {
                                freeIdentityIDs.Remove(freeIdentityID);

                                break;
                            }
                        }
                    }
                } while (isContinue);
            }

            uint value = NetworkIdentity.SetLocalPlayer(type, server.driver.GetConnectionState(connection) == NetworkConnection.State.Connected);

            var identity = prefab.Instantiate(
                rotation,
                position,
                id,
                value);

            identity._host = null;

            if (__identities == null)
                __identities = new Dictionary<uint, NetworkIdentityComponent>();

            __identities.Add(id, identity);

            return identity;
        }

        public void Destroy(uint id)
        {
            var identity = __identities[id];

            int type = identity.type;

            if (__freeIdentityIDs == null)
                __freeIdentityIDs = new Dictionary<int, HashSet<uint>>();

            if(!__freeIdentityIDs.TryGetValue(type, out var freeIdentityIDs))
            {
                freeIdentityIDs = new HashSet<uint>();

                __freeIdentityIDs[type] = freeIdentityIDs;
            }

            freeIdentityIDs.Add(id);

            Destroy(identity.gameObject);

            __identities.Remove(id);
        }

        public bool TryGetNode(uint id, out int node)
        {
            return rpcManager.TryGetNode(id, out node);
        }

        public bool NodeContains(int node, uint id)
        {
            var enumerator = rpcManager.GetNodeIDs(node);
            while (enumerator.MoveNext())
            {
                if (enumerator.Current == id)
                    return true;
            }

            return false;
        }

        public void UnregisterHandler(uint messageType)
        {
            if (__handlers != null)
                __handlers.Remove(messageType);// MsgType.Disconnect);
        }

        public void RegisterHandler(uint messageType, NetworkServerHandler handler)
        {
            if (__handlers == null)
                __handlers = new Dictionary<uint, NetworkServerHandler>();

            __handlers[messageType] = handler;
        }

        public void ExclusiveTransactionStart()
        {
            if (isExclusivingTransaction)
                throw new InvalidOperationException();

            isExclusivingTransaction = true;
        }

        public void ExclusiveTransactionEnd()
        {
            if (!isExclusivingTransaction)
                throw new InvalidOperationException();

            if (__exclusiveTransactionIdentityIDs != null)
            {
                NetworkIdentityComponent identity;
                foreach (uint id in __exclusiveTransactionIdentityIDs)
                {
                    identity = GetIdentity(id);
                    if (identity is NetworkIdentityComponent)
                    {
                        if (identity._onCreate != null)
                            identity._onCreate();
                    }
                }

                __exclusiveTransactionIdentityIDs.Clear();
            }

            isExclusivingTransaction = false;
        }

        public void Configure(NativeArray<NetworkPipelineType> pipelineTypes)
        {
            var driver = server.driver;
            int numPipelineTypes = pipelineTypes.Length;
            __pipelines = new NativeArray<NetworkPipeline>(numPipelineTypes, Allocator.Persistent, NativeArrayOptions.UninitializedMemory);
            for (int i = 0; i < numPipelineTypes; ++i)
                __pipelines[i] = driver.CreatePipeline(pipelineTypes[i]);
        }

        public bool Listen(ushort port, NetworkFamily family = NetworkFamily.Ipv4)
        {
            //Shutdown();
            if (server.isListening)
                return false;

            server.Listen(port, family);

            return true;
        }

        public void Shutdown()
        {
            ref var entityManager = ref this.entityManager;
            if(__freeIdentityIDs != null)
            {
                foreach (var freeIdentityIDs in __freeIdentityIDs.Values)
                {
                    foreach(var freeIdentityID in freeIdentityIDs)
                        entityManager.Unregister(freeIdentityID);

                    freeIdentityIDs.Clear();
                }

                //__freeIdentityIDs.Clear();
            }

            if (__identities != null)
            {
                foreach (var identity in __identities)
                {
                    entityManager.Unregister(identity.Key);

                    Destroy(identity.Value.gameObject);
                }

                __identities.Clear();
            }

            server.DisconnectAllConnections();
        }

        public void Disconnect(in NetworkConnection connection)
        {
            server.Disconnect(connection);
        }

        //注意：这些链接没有断开
        public bool Unregister(uint id, NetworkReader reader)
        {
            return __Unregister(_defaultChannel, id, rpcManager.GetConnection(id), reader);
        }

        public bool Register(uint id, NetworkReader reader)
        {
            if (!__Register(
                reader, 
                default, 
                ref id, 
                out int node,
                out int layerMask,
                out float visibilityDistance))
                return false;

            return __Register(
                id,
                _defaultChannel, 
                node, 
                layerMask, 
                visibilityDistance, 
                default);
        }

        public bool Move(uint id, int node, int layerMask, float visibilityDistance)
        {
            var commander = this.commander;
            if(commander.BeginCommand(id, NetworkPipeline.Null, server.driver, out var writer))
            {
                __SendRegisterMessage(ref writer, id);

                int length = writer.Length;

                __SendUnregisterMessage(ref writer, id);

                int value = commander.EndCommandMove(length, node, layerMask, visibilityDistance, writer);
                if (value < 0)
                {
                    Debug.LogError($"[EndRPC]{(StatusCode)value}");

                    return false;
                }

                return true;
            }

            Debug.LogError("[Move]Fail.");

            return false;
        }

        public bool BeginRPC(int channel, uint id, uint handle, out NetworkWriter writer)
        {
            if (commander.BeginCommand(id, __pipelines[channel], server.driver, out writer))
            {
                writer.WritePackedUInt(handle);

                return true;
            }

            Debug.LogError("[BeginRPC]Fail.");

            return false;
        }

        public int EndRPC(NetworkRPCType type, NetworkWriter writer, in NativeArray<uint> additionalIDs = default, in NativeArray<uint> maskIDs = default)
        {
            int value = commander.EndCommandRPC((int)type, writer, additionalIDs, maskIDs);
            if(value < 0)
                Debug.LogError($"[EndRPC]{(StatusCode)value}");
            
            return value;
        }

        public int EndRPC(NetworkWriter writer)
        {
            return EndRPC(NetworkRPCType.Normal, writer);
        }

        public int EndRPC(NetworkWriter writer, in NativeArray<uint> specifiedIDs)
        {
            return EndRPC(NetworkRPCType.None, writer, specifiedIDs);
        }

        public int EndRPCWithout(NetworkWriter writer, in NativeArray<uint> ids)
        {
            return EndRPC(NetworkRPCType.Normal, writer, default, ids);
        }

        public bool BeginSend(int channel, uint messageType, in NetworkConnection connection, out NetworkWriter writer)
        {
            var statusCode = server.BeginSend(__pipelines[channel], connection, messageType, out writer);
            if (StatusCode.Success == statusCode)
                return true;

            Debug.LogError($"[BeginSend]{statusCode}");

            return false;
        }

        public int EndSend(NetworkWriter writer)
        {
            int result = server.EndSend(writer);
            if (result >= 0)
                return result;

            Debug.LogError($"[EndSend]{((StatusCode)result)}");

            return result;
        }

        public bool Send(int channel, uint messageType, in NetworkConnection connection)
        {
            if (BeginSend(channel, messageType, connection, out var writer))
            {
                int result = EndSend(writer);
                if (result >= 0)
                    return true;
            }

            return false;
        }

        public bool Send<T>(int channel, uint messageType, in NetworkConnection connection, T message) where T : INetworkMessage
        {
            if (BeginSend(channel, messageType, connection, out var writer))
            {
                message.Serialize(ref writer);

                int result = EndSend(writer);
                if (result >= 0)
                    return true;
            }

            return false;
        }

        public bool Send<T>(int channel, uint messageType, uint id, T message) where T : INetworkMessage
        {
            if (commander.BeginCommand(id, __pipelines[channel], server.driver, out var writer))
            {
                message.Serialize(ref writer);

                var additionalIDs = new NativeArray<uint>(1, Allocator.Temp, NativeArrayOptions.UninitializedMemory);
                additionalIDs[0] = id;
                int value = commander.EndCommandRPC((int)messageType, writer, additionalIDs);
                additionalIDs.Dispose();
                if (value >= 0)
                    return true;

                Debug.LogError($"[EndRPC]{(StatusCode)value}");
            }
            else
                Debug.LogError("[BeginRPC]Fail.");

            return false;
        }

        private int __GetLayer(int type)
        {
            return __wrapper == null ? 0 : __wrapper.GetLayer(type);
        }

        private bool __Register(
            NetworkReader reader,
            in NetworkConnection connection,
            ref uint id, 
            out int node,
            out int layerMask,
            out float visibilityDistance)
        {
            if (__wrapper == null)
            {
                node = 0;
                layerMask = ~0;
                visibilityDistance = 0.0f;

                return true;
            }

            return __wrapper.Register(reader, connection, ref id, out node, out layerMask, out visibilityDistance);
        }

        private bool __Init(bool isNew, uint sourceID, uint destinationID, ref NetworkWriter writer)
        {
            return __wrapper != null && __wrapper.Init(isNew, sourceID, destinationID, ref writer);
        }

        private void __SendRegisterMessage(ref NetworkWriter writer, uint id)
        {
            if (__wrapper != null)
                __wrapper.SendRegisterMessage(ref writer, id);
        }

        private void __SendUnregisterMessage(ref NetworkWriter writer, uint id)
        {
            if (__wrapper != null)
                __wrapper.SendUnregisterMessage(ref writer, id);
        }

        private bool __Unregister(uint id, in NetworkConnection connection, NetworkReader reader)
        {
            return __wrapper != null && __wrapper.Unregister(id, connection, reader);
        }

        private bool __Unregister(int channel, uint id, in NetworkConnection connection, NetworkReader reader)
        {
            if (__identities != null && __identities.TryGetValue(id, out var identity))
            {
                int type = identity.type;

                identity.isLocalPlayer = false;

                if (__exclusiveTransactionIdentityIDs == null || !__exclusiveTransactionIdentityIDs.Remove(id))
                {
                    if (identity._onDestroy != null)
                        identity._onDestroy();
                }

                var commander = this.commander;
                if (__Unregister(id, connection, reader))
                {
                    if(identity != null)
                        identity._host = null;

                    if (commander.BeginCommand(id, __pipelines[channel], server.driver, out var writer))
                    {
                        __SendUnregisterMessage(ref writer, id);

                        int result = commander.EndCommandUnregister(writer);
                        if (result < 0)
                        {
                            Debug.LogError($"[EndCommandUnregister]{(StatusCode)result}");

                            return false;
                        }
                    }
                    else
                    {
                        Debug.LogError($"[BeginCommand] Fail");

                        return false;
                    }
                }
                else
                {
                    if (commander.BeginCommand(id, __pipelines[channel], server.driver, out var writer))
                    {
                        __SendUnregisterMessage(ref writer, id);
                        int result = commander.EndCommandDisconnect(type, writer);
                        if (result < 0)
                        {
                            Debug.LogError($"[EndCommandDisconnect]{(StatusCode)result}");

                            return false;
                        }
                    }
                    else
                    {
                        Debug.LogError($"[BeginCommand] Fail");

                        return false;
                    }
                }

                return true;
            }

            return false;
        }

        private bool __Register(int channel, uint id, in NetworkConnection connection, NetworkReader reader)
        {
            //if (server.connectionCount < maxPlayerCount)
            {
                uint originID = id;
                if (!__Register(
                    reader, 
                    connection, 
                    ref id,
                    out int node, 
                    out int layerMask, 
                    out float visibilityDistance))
                    return false;

                if (id != originID)
                {
                    var ids = server.ids;
                    ids[connection] = id;
                }

                return __Register(id, channel, node, layerMask, visibilityDistance, connection);
            }

            /*server.driver.Disconnect(connection);

            Debug.LogError("[Register Fail]Player count is out of range.");

            return false;*/
        }

        private bool __Register(
            uint id, 
            int channel, 
            int node,
            int layerMask, 
            float visibilityDistance, 
            in NetworkConnection connection)
        {
            var target = __identities[id];

            int type = target.type;

            var pipeline = __pipelines[channel];
            var driver = server.driver;
            if (commander.BeginCommand(id, pipeline, driver, out var writer))
            {
                __SendRegisterMessage(ref writer, id);

                int result;
                if ((INetworkHost)this == target._host)
                {
                    result = commander.EndCommandConnect(type, connection, writer);
                    if (result < 0)
                    {
                        Debug.LogError($"[EndCommandConnect]{(StatusCode)result}");

                        return false;
                    }

                    Move(id, node, layerMask, visibilityDistance);
                }
                else
                {
                    result = commander.EndCommandRegister(type, node, __GetLayer(type), layerMask, visibilityDistance, connection, writer);
                    if (result < 0)
                    {
                        Debug.LogError($"[EndCommandRegister]{(StatusCode)result}");

                        return false;
                    }
                }
            }
            else
            {
                Debug.LogError($"[BeginCommand]Fail");

                return false;
            }

#if DEBUG
            target.name = target.name.Replace("(Clone)", "(" + id + ')');
#endif

            /*NetworkIdentity identity;
            identity.id = id;
            identity.value = NetworkIdentity.SetLocalPlayer(type, driver.GetConnectionState(connection) == NetworkConnection.State.Connected);

            target.SetComponentData(identity);*/

            target._host = this;
            target.isLocalPlayer = driver.GetConnectionState(connection) == NetworkConnection.State.Connected;

            /*if (__identities == null)
                __identities = new Dictionary<uint, NetworkIdentityComponent>();

            __identities[id] = target;*/

            if (isExclusivingTransaction)
            {
                if (__exclusiveTransactionIdentityIDs == null)
                    __exclusiveTransactionIdentityIDs = new HashSet<uint>();

                __exclusiveTransactionIdentityIDs.Add(id);
            }
            else
            {
                if (target._onCreate != null)
                    target._onCreate();
            }

            return true;
        }

        private bool __RPC(uint id, in NetworkConnection connection, NetworkReader reader)
        {
            var identity = GetIdentity(id);
            if (identity == null)
            {
                Debug.LogError("[RPC Fail]Fail to get identity!");

                return false;
            }

            identity.InvokeHandler(reader.ReadPackedUInt(), connection, reader);

            return true;
        }

        private NetworkRPCController __GetController()
        {
            if (!__controller.isCreated)
                __controller = world.GetExistingSystemUnmanaged<NetworkRPCFactorySystem>().controller;

            return __controller;
        }

        void Awake()
        {
            __wrapper = GetComponent<INetworkServerWrapper>();
        }

        void OnDestroy()
        {
            if (__pipelines.IsCreated)
                __pipelines.Dispose();

            if (__ids.IsCreated)
                __ids.Dispose();

            if (__messages.IsCreated)
                __messages.Dispose();
        }

        void LateUpdate()
        {
            var server = this.server;

            NetworkServerHandler handler;
            var messages = server.messages;
            NetworkServerMessage message;
            int numMessages = messages.Length;
            for (int i = 0; i < numMessages; ++i)
            {
                message = messages[i];

                try
                {
                    switch (message.type)
                    {
                        case (uint)NetworkMessageType.Connect:
                            if (onConnect != null)
                                onConnect(message.connection);
                            break;
                        case (uint)NetworkMessageType.Disconnect:
                            if (onDisconnect != null)
                                onDisconnect(message.connection, (DisconnectReason)message.stream.ReadByte());
                            break;
                        default:
                            if (__handlers != null && __handlers.TryGetValue(message.type, out handler) && handler != null)
                                handler(message.connection, message.stream);
                            else
                                Debug.LogError($"The handler of message {message.type} is missing!");
                            break;
                    }
                }
                catch (Exception e)
                {
                    Debug.LogException(e.InnerException ?? e);
                }
            }

            if (__ids.IsCreated)
                __ids.Clear();
            else
                __ids = new NativeList<uint>(Allocator.Persistent);

            server.GetIDs(ref __ids);
            foreach (var id in __ids)
            {
                if (__messages.IsCreated)
                    __messages.Clear();
                else
                    __messages = new NativeList<NetworkServerMessage>(Allocator.Persistent);

                server.Receive(id, ref __messages);

                numMessages = __messages.Length;
                for (int i = 0; i < numMessages; ++i)
                {
                    message = __messages[i];

                    switch (message.type)
                    {
                        case (uint)NetworkMessageType.RPC:
                            __RPC(id, message.connection, message.stream);
                            break;
                        case (uint)NetworkMessageType.Register:
                            __Register(_defaultChannel, id, message.connection, message.stream);
                            break;
                        case (uint)NetworkMessageType.Unregister:
                            __Unregister(_defaultChannel, id, message.connection, message.stream);
                            break;
                    }
                }
            }

            if (onActive != null)
            {
                var activeEvents = commander.activeEventsReadOnly;
                NetworkRPCCommander.ActiveEvent activeEvent;
                int numActiveEvents = activeEvents.Length;
                for (int i = 0; i < numActiveEvents; ++i)
                {
                    activeEvent = activeEvents[i];

                    onActive(activeEvent.id, activeEvent.type == NetworkRPCCommander.ActiveEventType.Enable);
                }
            }

            var driver = server.driver;
            NetworkWriter writer;

            __ids.Clear();
            commander.GetInitIDs(ref __ids);

            int result;
            foreach (var id in __ids)
            {
                foreach (var initEvent in commander.GetInitEvents(id))
                {
                    if (commander.BeginCommand(id, NetworkPipeline.Null, driver, out writer))
                    {
                        if (__Init(initEvent.type == NetworkRPCCommander.InitType.New, id, initEvent.id, ref writer))
                        {
                            result = commander.EndCommandInit(initEvent.type, initEvent.id, writer);
                            if (result < 0)
                                Debug.LogError($"[EndCommandInit]{(StatusCode)result}");
                        }
                        else
                            commander.AbortCommand(writer);
                    }
                }
            }
        }

    }
}