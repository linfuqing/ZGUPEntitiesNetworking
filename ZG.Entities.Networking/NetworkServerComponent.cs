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
    public class NetworkServerComponent : MonoBehaviour, INetworkHost
    {
        public delegate void Handler(NetworkConnection connection, NetworkReader reader);

        public int maxPlayerCount = 64;

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
        internal int _receiveQueueCapacity = ReceiveQueueCapacity;
        [SerializeField]
        internal int _sendQueueCapacity = SendQueueCapacity;

        [SerializeField]
        internal string _worldName = "Server";

        public NetworkIdentityComponent[] prefabs;

        private World __world;
        private NetworkServerManager __manager;
        private NetworkRPCController __controller;

        private NativeArray<NetworkPipeline> __pipelines;
        private NativeList<uint> __ids;
        private NativeList<NetworkServer.Message> __messages;

        private Dictionary<uint, Handler> __handlers;
        private Dictionary<uint, NetworkIdentityComponent> __identities;

        private HashSet<uint> __exclusiveTransactionIdentityIDs;

        public event Action<NetworkConnection> onConnect;
        public event Action<NetworkConnection, DisconnectReason> onDisconnect;

        public event Action<uint, bool> onActive;

        public bool isConfigured => __pipelines.IsCreated;

        public bool isExclusivingTransaction
        {
            get;

            private set;
        }

        public int connectionCount => server.connectionCount;

        public World world
        {
            get
            {
                if(__world == null)
                    __world = WorldUtility.GetOrCreateWorld(_worldName);

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

        public NetworkRPCManager<int> manager
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

        public bool IsExclusiveTransaction(uint id) => __exclusiveTransactionIdentityIDs != null && __exclusiveTransactionIdentityIDs.Contains(id);

        public bool IsPlayer(uint id)
        {
            return manager.GetConnection(id).IsCreated;
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
            return manager.GetConnection(id);
        }

        public NetworkIdentityComponent GetIdentity(uint id)
        {
            return __identities != null && __identities.TryGetValue(id, out var value) ? value : null;
        }

        public uint CreateNewID()
        {
            return server.CreateNewID();
        }

        public void UnregisterHandler(uint messageType)
        {
            if (__handlers != null)
                __handlers.Remove(messageType);// MsgType.Disconnect);
        }

        public void RegisterHandler(uint messageType, Handler handler)
        {
            if (__handlers == null)
                __handlers = new Dictionary<uint, Handler>();

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

        public void Listen(ushort port, NetworkFamily family = NetworkFamily.Ipv4)
        {
            Shutdown();

            server.Listen(port, family);
        }

        public void Shutdown()
        {
            if (__identities != null)
            {
                foreach(var identity in __identities.Values)
                    _Destroy(identity);

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
            return __Unregister(id, manager.GetConnection(id), reader);
        }

        public bool Register(NetworkIdentityComponent identity, NetworkReader reader)
        {
            uint id = 0;
            if (!_Register(
                reader, 
                default, 
                ref identity, 
                ref id, 
                out int type, 
                out int node,
                out int layerMask,
                out float visibilityDistance))
                return false;

            return __Register(
                id, 
                type, 
                node, 
                layerMask, 
                visibilityDistance, 
                default, 
                identity);
        }

        public bool Move(uint id, int node, int layerMask, float visibilityDistance)
        {
            var commander = this.commander;
            if(commander.BeginCommand(id, NetworkPipeline.Null, server.driver, out var writer))
            {
                _SendRegisterMessage(ref writer, id);

                int length = writer.Length;

                _SendUnregisterMessage(ref writer, id);

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

        protected void LateUpdate()
        {
            var server = this.server;

            Handler handler;
            var messages = server.messagesReadOnly;
            NetworkServer.Message message;
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
                    __messages = new NativeList<NetworkServer.Message>(Allocator.Persistent);

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
                            __Register(id, message.connection, message.stream);
                            break;
                        case (uint)NetworkMessageType.Unregister:
                            __Unregister(id, message.connection, message.stream);
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
                        if (_Init(initEvent.type == NetworkRPCCommander.InitType.New, id, initEvent.id, ref writer))
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

        protected void OnDestroy()
        {
            if (__pipelines.IsCreated)
                __pipelines.Dispose();

            if (__ids.IsCreated)
                __ids.Dispose();

            if (__messages.IsCreated)
                __messages.Dispose();
        }

        protected virtual int _GetLayer(int type)
        {
            return 0;
        }

        protected virtual bool _Register(
            NetworkReader reader,
            in NetworkConnection connection,
            ref NetworkIdentityComponent identity,
            ref uint id, 
            out int type,
            out int node,
            out int layerMask,
            out float visibilityDistance)
        {
            type = 0;
            node = 0;
            layerMask = ~0;
            visibilityDistance = 0.0f;

            return true;
        }

        protected virtual NetworkIdentityComponent _Instantiate(uint id, int type)
        {
            int numPrefabs = prefabs == null ? 0 : prefabs.Length;
            return numPrefabs > type ? Instantiate(prefabs[type]) : (numPrefabs == 1 ? Instantiate(prefabs[0]) : null);
        }

        protected virtual void _Destroy(NetworkIdentityComponent identity)
        {
            if (identity != null)
                Destroy(identity.gameObject);
        }

        protected virtual bool _Init(bool isNew, uint sourceID, uint destinationID, ref NetworkWriter writer)
        {
            return false;
        }

        protected virtual void _SendRegisterMessage(ref NetworkWriter writer, uint id)
        {

        }

        protected virtual void _SendUnregisterMessage(ref NetworkWriter writer, uint id)
        {

        }

        protected virtual bool _Unregister(uint id, in NetworkConnection connection, NetworkReader reader)
        {
            return true;
        }

        private bool __Unregister(uint id, in NetworkConnection connection, NetworkReader reader, int channel = 0)
        {
            if (__identities != null && __identities.TryGetValue(id, out var identity))
            {
                bool isCreated = __exclusiveTransactionIdentityIDs == null || !__exclusiveTransactionIdentityIDs.Remove(id);
                int type = 0;
                if (identity is NetworkIdentityComponent)
                {
                    type = identity.type;

                    identity.isLocalPlayer = false;

                    if (isCreated)
                    {
                        if (identity._onDestroy != null)
                            identity._onDestroy();
                    }
                }

                var commander = this.commander;
                if (_Unregister(id, connection, reader))
                {
                    __identities.Remove(id);

                    _Destroy(identity);

                    if (commander.BeginCommand(id, __pipelines[channel], server.driver, out var writer))
                    {
                        _SendUnregisterMessage(ref writer, id);

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
                        _SendUnregisterMessage(ref writer, id);
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

        private bool __Register(uint id, in NetworkConnection connection, NetworkReader reader)
        {
            if (server.connectionCount < maxPlayerCount)
            {
                uint originID = id;
                NetworkIdentityComponent identity = null;
                if (!_Register(
                    reader, 
                    connection, 
                    ref identity,
                    ref id,
                    out int type, 
                    out int node, 
                    out int layerMask, 
                    out float visibilityDistance))
                    return false;

                if (id != originID)
                {
                    var ids = server.ids;
                    ids[connection] = id;
                }

                __Register(id, type, node, layerMask, visibilityDistance, connection, identity);

                return true;
            }

            server.driver.Disconnect(connection);

            Debug.LogError("[Register Fail]Player count is out of range.");

            return false;
        }

        public bool __Register(
            uint id, 
            int type,
            int node,
            int layerMask, 
            float visibilityDistance, 
            in NetworkConnection connection,
            NetworkIdentityComponent instance, 
            int channel = 0)
        {
            bool isReconnect;
            var target = GetIdentity(id);
            if (instance == null)
            {
                if (target == null)
                {
                    target = _Instantiate(id, type);
                    if (target == null)
                    {
                        Debug.LogError("Register Fail: Instantiate Error.");

                        return false;
                    }

                    isReconnect = false;
                }
                else
                {
                    var targetConnection = manager.GetConnection(target.id);
                    if (targetConnection.IsCreated && targetConnection != connection)
                    {
                        Debug.LogError("Register Fail: Index Conflict.");

                        return false;
                    }

                    isReconnect = true;
                }
            }
            else
            {
                if (target == null)
                    isReconnect = false;
                else
                {
                    if (target != instance)
                    {
                        Debug.LogError($"{instance} Copy From {target}");

                        //instance.SetComponentData(target.identity);

                        if (target._onDestroy != null)
                            target._onDestroy();

                        _Destroy(target);
                    }

                    isReconnect = true;
                }

                target = instance;
            }

            var pipeline = __pipelines[channel];
            var driver = server.driver;
            if (commander.BeginCommand(id, pipeline, driver, out var writer))
            {
                _SendRegisterMessage(ref writer, id);

                int result;
                if (isReconnect)
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
                    result = commander.EndCommandRegister(type, node, _GetLayer(type), layerMask, visibilityDistance, connection, writer);
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

            NetworkIdentity identity;
            identity.id = id;
            identity.value = NetworkIdentity.SetLocalPlayer(type, driver.GetConnectionState(connection) == NetworkConnection.State.Connected);

            target._host = this;

            target.SetComponentData(identity);

            if (__identities == null)
                __identities = new Dictionary<uint, NetworkIdentityComponent>();

            __identities[id] = target;

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


        public NetworkRPCController __GetController()
        {
            if (!__controller.isCreated)
                __controller = world.GetOrCreateSystemUnmanaged<NetworkRPCFactorySystem>().controller;

            return __controller;
        }

    }
}