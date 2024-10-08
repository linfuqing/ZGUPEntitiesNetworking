using System;
using System.Collections.Generic;
using Unity.Collections;
using Unity.Entities;
using Unity.Networking.Transport;
using Unity.Networking.Transport.Error;
using UnityEngine;
using NetworkReader = Unity.Collections.DataStreamReader;
using NetworkWriter = Unity.Collections.DataStreamWriter;
using static Unity.Networking.Transport.NetworkParameterConstants;

namespace ZG
{
    public interface INetworkClientCreateRequest
    {
        bool isDone { get; }

        NetworkIdentityComponent instance { get; }
    }

    public interface INetworkClientWrapper
    {
        void Init(NetworkIdentityComponent identity);

        NetworkIdentityComponent GetIdentity(uint id);

        void Destroy(bool isLocalPlayer, uint id, int type, NetworkReader reader, NetworkIdentityComponent identity);

        bool Create(bool isLocalPlayer, uint id, int type, NetworkReader reader, ref INetworkClientCreateRequest request);
    }

    public interface INetworkClient
    {
        event Action onConnect;
        event Action<DisconnectReason> onDisconnect;

        bool isBusy { get; }

        bool isConfigured { get; }

        bool isConnected { get; }

        NetworkConnection.State connectionState { get; }

        int identityCount { get; }

        ref NetworkEntityManager entityManager { get; }

        NetworkIdentityComponent GetIdentity(uint id);

        void Configure(NativeArray<NetworkPipelineType> pipelineTypes);

        bool Connect(in NetworkEndpoint endPoint);

        void UnregisterHandler(uint messageType);

        void RegisterHandler(uint messageType, Action<NetworkReader> handler);

        bool Send<T>(int channel, uint messageType, in T message) where T : INetworkMessage;

        bool Send(int channel, uint messageType);

        bool BeginSend(int channel, uint messageType, out NetworkWriter writer);

        int EndSend(NetworkWriter writer);

        bool Register();

        bool Register<T>(in T message) where T : INetworkMessage;

        bool Unregister<T>(in T message) where T : INetworkMessage;

        void Shutdown();

        void InitAndCreateSync();
    }

    public sealed class NetworkClientComponent : MonoBehaviour, INetworkHost, INetworkClient
    {
        private struct CreateRequest : INetworkClientCreateRequest
        {
            public bool isDone => true;

            public NetworkIdentityComponent instance { get; set; }
        }

        private struct BufferToKeep
        {
            public short handle;
            public byte[] bytes;

            public BufferToKeep(short handle, byte[] bytes)
            {
                this.handle = handle;
                this.bytes = bytes;
            }
        }

        private struct BufferToCreate
        {
            //public uint order;
            public uint identity;
            public INetworkClientCreateRequest request;
        }

        private struct BufferToInit
        {
            public uint identity;
            public NetworkIdentityComponent instance;
        }

        public event Action onConnect;
        public event Action<DisconnectReason> onDisconnect;

        [SerializeField]
        internal string _worldName = "Client";

        public long maxUpdateTicksPerFrame = TimeSpan.TicksPerMillisecond << 6;

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

        private bool __isInitAndCreateSync;
        private bool __isBusy;
        
#if DEBUG
        private int __rpcTimes;
        private int __rpcSize;
        private int __rpcFrame;
#endif
        
        private INetworkClientWrapper __wrapper;
        private World __world;
        private NetworkClientManager __manager;
        private NativeArray<NetworkPipeline> __pipelines;
        private NativeList<uint> __ids;
        private Dictionary<uint, Action<NetworkReader>> __handlers;
        //private SortedList<uint, uint> __indicesToCreate;
        private Dictionary<uint, BufferToInit> __buffersToInit;
        private Dictionary<uint, BufferToCreate> __buffersToCreate;
        private Dictionary<uint, NetworkIdentityComponent> __identities;

        public NetworkConnection.State connectionState => client.connectionState;

        public bool isConnected
        {
            get;

            private set;
        }

        public bool isConfigured => __pipelines.IsCreated;

        public bool isBusy
        {
            get
            {
                return __isBusy ||
                    __buffersToCreate != null && __buffersToCreate.Count > 0 || 
                    __buffersToInit != null && __buffersToInit.Count > 0;
            }
        }

        public int identityCount
        {
            get
            {
                return __identities == null ? 0 : __identities.Count;
            }
        }

        //public int rtt => 0;//client.GetRTT();

        public World world
        {
            get
            {
                if(__world == null)
                    __world = WorldUtility.GetWorld(_worldName);

                return __world;
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

        public NetworkClient client
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

                    __manager = NetworkClientSystem.CreateManager(world.Unmanaged, settings);
                }

                __manager.lookupJobManager.CompleteReadWriteDependency();

                return __manager.client;
            }
        }

        public IEnumerable<NetworkIdentityComponent> identities
        {
            get
            {
                return __identities == null ? null : __identities.Values;
            }
        }

        public NetworkIdentityComponent GetIdentity(uint id)
        {
            if (__identities == null)
                return null;

            NetworkIdentityComponent result;
            if (__identities.TryGetValue(id, out result))
                return result;

            return null;
        }

        public void InitAndCreateSync()
        {
            __isInitAndCreateSync = true;
        }

        public NetworkIdentityComponent InitNext()
        {
            if (__buffersToInit != null)
            {
                var enumerator = __buffersToInit.GetEnumerator();

                while (enumerator.MoveNext())
                {
                    var pair = enumerator.Current;
                    
                    enumerator.Dispose();

                    uint id = pair.Key;
                    __buffersToInit.Remove(id);

                    var bufferToInit = pair.Value;

                    UnityEngine.Profiling.Profiler.BeginSample(bufferToInit.instance.name);

                    bufferToInit.instance._host = this;

                    bufferToInit.instance.isLocalPlayer = NetworkIdentity.IsLocalPlayer(bufferToInit.identity);

                    /*NetworkIdentity identity;
                    identity.id = id;
                    identity.value = bufferToInit.identity;
                    bufferToInit.instance.identity = identity;*/

                    //Debug.Log($"Init Identity {id} : {bufferToInit.instance.name} : {bufferToInit.instance.entity}");

                    if (__identities == null)
                        __identities = new Dictionary<uint, NetworkIdentityComponent>();

                    __identities.Add(id, bufferToInit.instance);

#if UNITY_EDITOR
                    UnityEngine.Profiling.Profiler.BeginSample("[Editor Only]Replace Name");

                    if (bufferToInit.instance.isLocalPlayer)
                        UnityEngine.Assertions.Assert.IsTrue(bufferToInit.instance.name.Contains("RZ"),
                            bufferToInit.instance.name);

                    bufferToInit.instance.name =
                        System.Text.RegularExpressions.Regex.Replace(bufferToInit.instance.name,
                            @"\(((Clone)|(\d+))\)", "(" + id + ')');

                    UnityEngine.Profiling.Profiler.EndSample();
#endif

                    //instance.gameObject.SetActive(false);

                    __Init(bufferToInit.instance);

                    if (bufferToInit.instance._onCreate != null)
                    {
                        UnityEngine.Profiling.Profiler.BeginSample("onCreate");
                        bufferToInit.instance._onCreate();
                        UnityEngine.Profiling.Profiler.EndSample();
                    }

                    __RPC(id);

                    UnityEngine.Profiling.Profiler.EndSample();

                    return bufferToInit.instance;
                }
                
                enumerator.Dispose();
            }

            return null;
        }

        public NetworkIdentityComponent CreateNext(bool isSync)
        {
            if (__buffersToCreate != null)
            {
                var enumerator = __buffersToCreate /*__indicesToCreate*/.GetEnumerator();
                while (enumerator.MoveNext())
                {
                    var pair = enumerator.Current;

                    //uint id = pair.Value;
                    var bufferToCreate = pair.Value; //__bufferToCreate[id];
                    if (isSync || bufferToCreate.request.isDone)
                    {
                        enumerator.Dispose();

                        uint id = pair.Key;
                        __buffersToCreate.Remove(id);
                        //__indicesToCreate.Remove(pair.Key);

                        //if (__identities == null)
                        //     __identities = new Dictionary<uint, NetworkIdentityComponent>();

                        var instance = bufferToCreate.request.instance;
                        if (instance == null)
                            Debug.LogError($"Instantiate Fail.(Identity: {bufferToCreate.identity}, id:{id})");
                        else
                        {
                            BufferToInit bufferToInit;
                            bufferToInit.identity = bufferToCreate.identity;
                            bufferToInit.instance = instance;

                            if (__buffersToInit == null)
                                __buffersToInit = new Dictionary<uint, BufferToInit>();

                            __buffersToInit.Add(id, bufferToInit);

                            /*instance._host = this;

                            NetworkIdentity identity;
                            identity.id = id;
                            identity.value = bufferToCreate.identity;
                            instance.SetComponentData(identity);

                            __identities.Add(id, instance);

#if DEBUG
                            if (instance.isLocalPlayer)
                                UnityEngine.Assertions.Assert.IsTrue(instance.name.Contains("RZ"), instance.name);

                            instance.name = System.Text.RegularExpressions.Regex.Replace(instance.name, @"\(((Clone)|(\d+))\)", "(" + id + ')');
#endif

                            //instance.gameObject.SetActive(false);

                            _Init(instance);

                            if (instance._onCreate != null)
                                instance._onCreate();

                            __RPC(id);*/
                        }

                        return instance;
                    }
                }
                
                enumerator.Dispose();
            }

            return null;
        }

        public void Configure(NativeArray<NetworkPipelineType> pipelineTypes)
        {
            var client = this.client;
            int numPipelineTypes = pipelineTypes.Length;
            __pipelines = new NativeArray<NetworkPipeline>(numPipelineTypes, Allocator.Persistent, NativeArrayOptions.UninitializedMemory);
            for (int i = 0; i < numPipelineTypes; ++i)
                __pipelines[i] = client.CreatePipeline(pipelineTypes[i]);
        }

        public bool Connect(in NetworkEndpoint endPoint)
        {
            UnityEngine.Assertions.Assert.IsTrue(endPoint.IsValid);

            __Shutdown();

            client.Connect(endPoint);

            return true;
        }

        public void Awake()
        {
            __wrapper = GetComponent<INetworkClientWrapper>();
        }

        public void Shutdown()
        {
            __Shutdown();
        }

        public void UnregisterHandler(uint messageType)
        {
            if (__handlers != null)
                __handlers.Remove(messageType);// MsgType.Disconnect);
        }

        public void RegisterHandler(uint messageType, Action<NetworkReader> handler)
        {
            if (__handlers == null)
                __handlers = new Dictionary<uint, Action<NetworkReader>>();

            __handlers[messageType] = handler;
        }

        public bool Register()
        {
            var client = this.client;
            var statusCode = client.BeginSendRegister(__pipelines[0], out var writer);
            if (statusCode == StatusCode.Success)
            {
                client.EndSend(writer);

                return true;
            }

            Debug.LogError($"Register Fail: {statusCode}");

            return false;
        }

        public bool Register<T>(in T message) where T : INetworkMessage
        {
            var client = this.client;
            var statusCode = client.BeginSendRegister(__pipelines[0], out var writer);
            if (statusCode == StatusCode.Success)
            {
                message.Serialize(ref writer);

                client.EndSend(writer);

                return true;
            }

            Debug.LogError($"Register Fail: {statusCode}");

            return false;
        }

        public bool Unregister<T>(in T message) where T : INetworkMessage
        {
            var client = this.client;
            var statusCode = client.BeginSendUnregister(__pipelines[0], out var writer);
            if (statusCode == StatusCode.Success)
            {
                message.Serialize(ref writer);

                client.EndSend(writer);

                return true;
            }

            Debug.LogError($"Unregister Fail: {statusCode}");

            return false;
        }

        public bool Send<T>(int channel, uint messageType, in T message) where T : INetworkMessage
        {
            var client = this.client;
            var statusCode = client.BeginSend(__pipelines[channel], messageType, out var writer);
            if (statusCode == StatusCode.Success)
            {
                message.Serialize(ref writer);

                client.EndSend(writer);

                return true;
            }

            Debug.LogError($"[Send]{statusCode}");

            return false;
        }

        public bool Send(int channel, uint messageType)
        {
            var client = this.client;
            var statusCode = client.BeginSend(__pipelines[channel], messageType, out var writer);
            if (statusCode == StatusCode.Success)
            {
                client.EndSend(writer);

                return true;
            }

            Debug.LogError($"[Send]{statusCode}");

            return false;
        }

        public bool BeginSend(int channel, uint messageType, out NetworkWriter writer)
        {
            var statusCode = client.BeginSend(__pipelines[channel], messageType, out writer);
            if (StatusCode.Success == statusCode)
                return true;

            Debug.LogError($"[BeginSend]{statusCode}");

            return false;
        }

        public int EndSend(NetworkWriter writer)
        {
            int result = client.EndSend(writer);
            if(result <= 0)
                Debug.LogError($"[EndSend]{(StatusCode)result}");

            return result;
        }

        public bool BeginRPC(int channel, uint id, uint handle, out NetworkWriter writer)
        {
            var statusCode = client.BeginSendRPC(__pipelines[channel], id, out writer);
            if (statusCode == StatusCode.Success)
            {
                writer.WritePackedUInt(handle);

                return true;
            }

            Debug.LogError($"[BeginRPC]{statusCode}");

            return false;
        }

        public int EndRPC(NetworkWriter writer)
        {
            /*if (writer.Length < 3)
            {
                Debug.LogError("Error Writer");

                client.AbortSend(writer);

                return -1;
            }*/

            int result = client.EndSend(writer);

#if DEBUG
            if (result > 0)
            {
                int rpcFrame = Time.frameCount;
                if (rpcFrame != __rpcFrame)
                {
                    __rpcFrame = rpcFrame;

                    __rpcTimes = 0;

                    __rpcSize = 0;
                }
                    
                ++__rpcTimes;
                __rpcSize += writer.Length;
            }
            else
                Debug.LogError($"[EndRPC]{(StatusCode)result},times : {__rpcTimes},sizes : {__rpcSize}");
#else
            if(result <= 0)
                Debug.LogError($"[EndRPC]{(StatusCode)result}");
#endif

            return result;
        }

        private void __Init(NetworkIdentityComponent identity)
        {
            UnityEngine.Profiling.Profiler.BeginSample($"Init");
            if (__wrapper == null)
                identity.gameObject.SetActive(true);
            else
                __wrapper.Init(identity);
            UnityEngine.Profiling.Profiler.EndSample();
        }

        private NetworkIdentityComponent __GetIdentity(uint id)
        {
            UnityEngine.Profiling.Profiler.BeginSample("GetIdentity");
            NetworkIdentityComponent result;
            if (__wrapper == null)
                result = __identities != null && __identities.TryGetValue(id, out var instance) ? instance : null;
            else
                result = __wrapper.GetIdentity(id);
            UnityEngine.Profiling.Profiler.EndSample();

            return result;
        }

        private void __Destroy(bool isLocalPlayer, uint id, int type, NetworkReader reader, NetworkIdentityComponent identity)
        {
            UnityEngine.Profiling.Profiler.BeginSample("Destroy");
            if(__wrapper == null)
                Destroy(identity.gameObject);
            else
                __wrapper.Destroy(isLocalPlayer, id, type, reader, identity);
            UnityEngine.Profiling.Profiler.EndSample();
        }

        private bool __Create(bool isLocalPlayer, int type, uint id, NetworkReader reader, ref INetworkClientCreateRequest request)
        {
            if (__wrapper == null)
            {
                return false;
                /*if (request != null)
                    return false;

                var createRequest = new CreateRequest();

                var prefab = prefabs == null || type < 0 || type >= prefabs.Length ? null : prefabs[type];
                var instance = prefab == null ? null : Instantiate(prefab);

                createRequest.instance = instance;

                request = createRequest;

                return true;*/
            }

            UnityEngine.Profiling.Profiler.BeginSample("Create");
            var result = __wrapper.Create(isLocalPlayer, id, type, reader, ref request);
            UnityEngine.Profiling.Profiler.EndSample();

            return result;
        }

        private bool __Create(uint id, uint identity, NetworkReader reader)
        {
            if (__identities != null && __identities.ContainsKey(id))
            {
                Debug.LogError("[__Create]Fail.");

                return false;
            }

            if (__buffersToCreate == null)
                __buffersToCreate = new Dictionary<uint, BufferToCreate>();

            if(__buffersToCreate.TryGetValue(id, out var buffer))
            {
                if (buffer.identity == identity)
                    return false;
            }
            /*else
            {
                ++__createOrder;

                if (__indicesToCreate == null)
                    __indicesToCreate = new SortedList<uint, uint>();

                __indicesToCreate.Add(__createOrder, id);

                buffer.order = __createOrder;
            }*/

            buffer.identity = identity;

            if (__Create(NetworkIdentity.IsLocalPlayer(identity), NetworkIdentity.GetType(identity), id, reader, ref buffer.request))
            {
                __buffersToCreate[id] = buffer;

                return true;
            }

            Debug.LogError("[__Create]Fail.");

            return false;
        }

        private void __RPC(uint id)
        {
            if (__buffersToCreate != null && __buffersToCreate.ContainsKey(id) || 
                __buffersToInit != null && __buffersToInit.ContainsKey(id))
                return;

            try
            {
                var client = this.client;
                using (var buffer = client.Receive(id))
                {
                    while (true)
                    {
                        switch (buffer.ReadMessage(out var reader, out uint identity))
                        {
                            case NetworkMessageType.RPC:
                                UnityEngine.Profiling.Profiler.BeginSample("RPC");
                                
                                var instance = __GetIdentity(id);
                                if (instance != null)
                                {
                                    UnityEngine.Profiling.Profiler.BeginSample(instance.name);
                                
                                    instance.InvokeHandler(reader.ReadPackedUInt(), client.connection, reader);
                                    
                                    UnityEngine.Profiling.Profiler.EndSample();
                                }
                                else
                                    Debug.LogError($"RPC Error: {id} : {reader.ReadPackedUInt()}");
                                
                                UnityEngine.Profiling.Profiler.EndSample();
                                break;
                            case NetworkMessageType.Register:
                                UnityEngine.Profiling.Profiler.BeginSample("Register");
                                bool result = __Create(id, identity, reader);
                                UnityEngine.Profiling.Profiler.EndSample();
                                
                                if(result)
                                    return;

                                break;
                            case NetworkMessageType.Unregister:
                                UnityEngine.Profiling.Profiler.BeginSample("Unregister");

                                if (__identities != null && __identities.TryGetValue(id, out instance))
                                {
                                    UnityEngine.Profiling.Profiler.BeginSample(instance.name);

                                    if (instance is NetworkIdentityComponent)
                                    {
                                        UnityEngine.Profiling.Profiler.BeginSample("onDestroy");

                                        if (instance._onDestroy != null)
                                            instance._onDestroy();
                                        
                                        UnityEngine.Profiling.Profiler.EndSample();
                                    }

                                    __identities.Remove(id);

                                    __Destroy(
                                        instance.isLocalPlayer,
                                        id,
                                        instance.type,
                                        reader, 
                                        instance);
                                    UnityEngine.Profiling.Profiler.EndSample();
                                }
                                else
                                    Debug.LogError($"Unregister Error: {id}");
                                
                                UnityEngine.Profiling.Profiler.EndSample();
                                break;
                            default:
                                return;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Debug.LogException(e);
            }
        }

        private void __Clear()
        {
            if (__identities != null)
            {
                NetworkIdentityComponent instance;
                foreach (var pair in __identities)
                {
                    instance = pair.Value;

                    if (instance is NetworkIdentityComponent)
                    {
                        if (instance._onDestroy != null)
                            instance._onDestroy();
                    }

                    __Destroy(
                        instance.isLocalPlayer,
                        pair.Key,
                        instance.type,
                        default, 
                        instance);
                }

                __identities.Clear();
            }

            if (__buffersToInit != null)
            {
                BufferToInit bufferToInit;
                foreach (var pair in __buffersToInit)
                {
                    bufferToInit = pair.Value;
                    __Destroy(
                        NetworkIdentity.IsLocalPlayer(bufferToInit.identity),
                        pair.Key,
                        NetworkIdentity.GetType(bufferToInit.identity),
                        default,
                        bufferToInit.instance);
                }

                __buffersToInit.Clear();
            }

            if (__buffersToCreate != null)
            {
                BufferToCreate bufferToCreate;
                foreach (var pair in __buffersToCreate)
                {
                    bufferToCreate = pair.Value;
                    __Destroy(
                        NetworkIdentity.IsLocalPlayer(bufferToCreate.identity),
                        pair.Key,
                        NetworkIdentity.GetType(bufferToCreate.identity),
                        default, 
                        bufferToCreate.request.instance);
                }

                __buffersToCreate.Clear();
            }

            /*if (__indicesToCreate != null)
                __indicesToCreate.Clear();

            __createOrder = 0;*/

            isConnected = false;
        }

        private void __Connect()
        {
            isConnected = true;

            if (onConnect != null)
                onConnect();
        }

        private void __Disconnect(DisconnectReason reason)
        {
            __Clear();

            if (onDisconnect != null)
                onDisconnect(reason);
        }

        private void __Shutdown()
        {
            __Clear();

            var client = this.client;
            if (client.connectionState != NetworkConnection.State.Disconnected)
                client.Shutdown();
        }

        private bool __IsUpdate(long ticks)
        {
            return DateTime.UtcNow.Ticks - ticks < maxUpdateTicksPerFrame;
        }

        void LateUpdate()
        {
            var client = this.client;

            UnityEngine.Profiling.Profiler.BeginSample("Client Messages");

            Action<NetworkReader> handler;
            var messages = client.messagesReadOnly;
            NetworkClient.Message message;
            int numMessages = messages.Length;
            for (int i = 0; i < numMessages; ++i)
            {
                message = messages[i];

                try
                {
                    switch (message.type)
                    {
                        case (uint)NetworkMessageType.Connect:
                            __Connect();
                            break;
                        case (uint)NetworkMessageType.Disconnect:
                            __Disconnect((DisconnectReason)message.stream.ReadByte());
                            break;
                        default:
                            if (__handlers != null && __handlers.TryGetValue(message.type, out handler) && handler != null)
                                handler(message.stream);
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
            
            UnityEngine.Profiling.Profiler.EndSample();

            if (isConnected/* && GameObjectEntity.IsAllEntitiesDeserialized(GameObjectEntity.DeserializedType.InstanceOnly)*/)
            {
                __isBusy = false;

                //GameObjectEntity.IsAllEntitiesDeserialized(GameObjectEntity.DeserializedType.InstanceOnly);

                //Debug.Log($"Deserialized Start {Time.frameCount}");

                if (__isInitAndCreateSync)
                {
                    __isInitAndCreateSync = false;

                    UnityEngine.Profiling.Profiler.BeginSample("Client Init");
                    while (InitNext() != null) ;
                    UnityEngine.Profiling.Profiler.EndSample();

                    UnityEngine.Profiling.Profiler.BeginSample("Client Create");
                    while (CreateNext(false) != null) ;
                    UnityEngine.Profiling.Profiler.EndSample();
                }
                else
                {
                    long ticks = DateTime.UtcNow.Ticks;

                    UnityEngine.Profiling.Profiler.BeginSample("Client Init");
                    while (__IsUpdate(ticks) && InitNext() != null)
                        __isBusy = true;
                    UnityEngine.Profiling.Profiler.EndSample();

                    UnityEngine.Profiling.Profiler.BeginSample("Client Create");
                    while (__IsUpdate(ticks) && CreateNext(false) != null)
                        __isBusy = true;
                    UnityEngine.Profiling.Profiler.EndSample();
                }

                UnityEngine.Profiling.Profiler.BeginSample("Client RPC IDs");
                
                if (__ids.IsCreated)
                    __ids.Clear();
                else
                    __ids = new NativeList<uint>(Allocator.Persistent);

                client.GetIDs(ref __ids);
                foreach (var id in __ids)
                    __RPC(id);
                
                UnityEngine.Profiling.Profiler.EndSample();
            }
        }

        void OnDestroy()
        {
            if (__pipelines.IsCreated)
                __pipelines.Dispose();

            if (__ids.IsCreated)
                __ids.Dispose();
        }

    }
}