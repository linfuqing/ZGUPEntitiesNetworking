using System;
using Unity.Jobs;
using Unity.Burst;
using Unity.Entities;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Mathematics;
using Unity.Networking.Transport;
using Unity.Networking.Transport.Error;
using Debug = UnityEngine.Debug;

namespace ZG
{
    public enum NetworkRPCType
    {
        None         = -1, 
        Normal       = -2,
        //SendOthers,
        SendSelfOnly = -3
    }

    public interface INetworkRPCMessage
    {
        int size { get; }

        bool IsVail(uint id);

        void Send(ref DataStreamWriter writer, uint sourceID, uint destinationID);
        
        void Dispose(uint sourceID, uint destinationID);
    }

    public struct NetworkRPCManager<T> where T : unmanaged, IEquatable<T>
    {
        public struct NodeIdentity
        {
            public int layer;
            public uint value;
        }

        internal struct Identity
        {
            public T node;
            public int layerMask;
            public float visibilityDistance;
            public NetworkConnection connection;
            public NetworkPipeline pipeline;
        }

        public struct ReadOnly
        {
            [ReadOnly]
            internal NativeParallelHashMap<uint, Identity> _identities;

            [ReadOnly]
            internal NativeParallelMultiHashMap<T, uint> _nodeIDs;

            [ReadOnly]
            internal NativeParallelMultiHashMap<T, NodeIdentity> _nodeIdentities;

            public NativeParallelMultiHashMap<T, uint>.Enumerator GetNodeIDs(uint id) => _nodeIDs.GetValuesForKey(_identities[id].node);

            public bool TryGetNode(uint id, out T node)
            {
                if (_identities.TryGetValue(id, out var identity))
                {
                    node = identity.node;

                    return true;
                }

                node = default;

                return false;
            }

            public NetworkPipeline GetPipeline(uint id)
            {
                if (_identities.TryGetValue(id, out var identity))
                    return identity.pipeline;

                return NetworkPipeline.Null;
            }

            public NetworkConnection GetConnection(uint id)
            {
                if (_identities.TryGetValue(id, out var identity))
                    return identity.connection;

                return default;
            }

            public NetworkConnection GetConnection(uint id, uint originID)
            {
                if (__GetIdentity(id, out int layer, out var identity))
                {
                    if(_identities.TryGetValue(originID, out var originIdentity) && (originIdentity.layerMask & (1 << layer)) != 0)
                    {
                        foreach(uint nodeID in _nodeIDs.GetValuesForKey(identity.node))
                        {
                            if(nodeID == id)
                                return identity.connection;
                        }
                    }
                }

                return default;
            }

            public bool RPC<TCommand>(
                uint id,
                in TCommand command,
                ref NativeParallelMultiHashMap<uint, TCommand> commands) where TCommand : unmanaged
            {
                if (!__GetIdentity(id, out int layer, out var identity))
                    return false;

                return __RPC(layer, identity.node, command, ref commands);
            }

            private bool __RPC<TCommand>(
                int layer,
                in T node,
                in TCommand command,
                ref NativeParallelMultiHashMap<uint, TCommand> commands) where TCommand : unmanaged
            {
                if (_nodeIDs.TryGetFirstValue(node, out var nodeID, out var iterator))
                {
                    Identity identity;
                    do
                    {
                        if (_identities.TryGetValue(nodeID, out identity) &&
                            (identity.layerMask & (1 << layer)) != 0 &&
                            identity.connection.IsCreated)
                            commands.Add(nodeID, command);
                    } while (_nodeIDs.TryGetNextValue(out nodeID, ref iterator));
                }

                return true;
            }

            private bool __GetIdentity(uint id, out int layer, out Identity identity)
            {
                if (_identities.TryGetValue(id, out identity))
                {
                    if (_nodeIdentities.TryGetFirstValue(identity.node, out var nodeIdentity, out var iterator))
                    {
                        do
                        {
                            if (nodeIdentity.value == id)
                            {
                                layer = nodeIdentity.layer;

                                return true;
                            }
                        } while (_nodeIdentities.TryGetNextValue(out nodeIdentity, ref iterator));
                    }
                }

                layer = 0;

                return false;
            }
        }

        private NativeParallelHashMap<uint, Identity> __identities;

        /// <summary>
        /// id可见的区域
        /// </summary>
        private NativeParallelMultiHashMap<uint, T> __idNodes;

        /// <summary>
        ///可以看见该区域的id
        /// </summary>
        private NativeParallelMultiHashMap<T, uint> __nodeIDs;

        /// <summary>
        /// 区域的所有Identity
        /// </summary>
        private NativeParallelMultiHashMap<T, NodeIdentity> __nodeIdentities;

        public NetworkRPCManager(AllocatorManager.AllocatorHandle allocator)
        {
            __identities = new NativeParallelHashMap<uint, Identity>(1, allocator);

            __idNodes = new NativeParallelMultiHashMap<uint, T>(1, allocator);

            __nodeIDs = new NativeParallelMultiHashMap<T, uint>(1, allocator);

            __nodeIdentities = new NativeParallelMultiHashMap<T, NodeIdentity>(1, allocator);
        }

        public void Dispose()
        {
            __identities.Dispose();
            __idNodes.Dispose();
            __nodeIDs.Dispose();
            __nodeIdentities.Dispose();
        }

        public ReadOnly AsReadOnly()
        {
            ReadOnly readOnly;
            readOnly._identities = __identities;
            readOnly._nodeIDs = __nodeIDs;
            readOnly._nodeIdentities = __nodeIdentities;

            return readOnly;
        }

        public int CountOfIDNodes() => __idNodes.Count();

        public bool TryGetNode(uint id, out T value)
        {
            if (__identities.TryGetValue(id, out var identity))
            {
                value = identity.node;

                return true;
            }

            value = default;

            return default;
        }

        public int GetLayerMask(uint id)
        {
            if (__identities.TryGetValue(id, out var identity))
                return identity.layerMask;

            return 0;
        }

        public NetworkPipeline GetPipeline(uint id)
        {
            if (__identities.TryGetValue(id, out var identity))
                return identity.pipeline;

            return NetworkPipeline.Null;
        }

        public NetworkConnection GetConnection(uint id)
        {
            if (__identities.TryGetValue(id, out var identity))
                return identity.connection;

            return default;
        }

        public NetworkConnection GetConnection(uint id, int layer)
        {
            if (__identities.TryGetValue(id, out var identity) && (identity.layerMask & (1 << layer)) != 0)
                return identity.connection;

            return default;
        }

        /// <summary>
        /// id可见的区域
        /// </summary>
        public NativeParallelMultiHashMap<uint, T>.Enumerator GetIDNodes(uint id)
        {
            return __idNodes.GetValuesForKey(id);
        }

        /// <summary>
        /// 可以看见该区域的id
        /// </summary>
        public NativeParallelMultiHashMap<T, uint>.Enumerator GetNodeIDs(in T value)
        {
            return __nodeIDs.GetValuesForKey(value);
        }

        /// <summary>
        /// 区域的所有Identity
        /// </summary>
        public NativeParallelMultiHashMap<T, NodeIdentity>.Enumerator GetNodeIdentities(in T value)
        {
            return __nodeIdentities.GetValuesForKey(value);
        }

        public bool IsActive<TDriver>(uint id, in TDriver driver) where TDriver : INetworkDriver
        {
            if (__GetIdentity(id, out int layer, out var identity, out _))
            {
                foreach (uint nodeID in __nodeIDs.GetValuesForKey(identity.node))
                {
                    if (NetworkConnection.State.Connected == driver.GetConnectionState(GetConnection(nodeID, layer)))
                        return true;
                }
            }

            return false;
        }

        public bool RPC<TDriver, TMessage>(
            uint id,
            ref TDriver driver,
            in NetworkPipeline pipeline,
            in TMessage message)
            where TDriver : INetworkDriver
            where TMessage : INetworkRPCMessage
        {
            if (!__GetIdentity(id, out int layer, out var identity, out _))
                return false;

            return __RPC(id, layer, identity.node, ref driver, pipeline, message);
        }

        public bool Reconnect<TDriver, TRegisterMessage, TUnregisterMessage>(
            uint id,
            ref TDriver driver, 
            in NetworkConnection connection, 
            in TRegisterMessage registerMessage,
            in TUnregisterMessage unregisterMessage)
            where TDriver : INetworkDriver
            where TRegisterMessage : INetworkRPCMessage
            where TUnregisterMessage : INetworkRPCMessage
        {
            if (!__identities.TryGetValue(id, out var identity))
                return false;

            if (identity.connection == connection)
                return true;

            /*string hehe = "hehe";
            foreach (var value in __idNodes.GetValuesForKey(id))
            {
                hehe += $" {value}";
            }

            UnityEngine.Debug.Log($"[RPCCommand]{id} Reconnect : {identity.node} : {identity.connection} : {connection} : {hehe}");*/

            bool isRegisteredOrigin = connection.IsCreated/*NetworkConnection.State.Connected == driver.GetConnectionState(connection)*/ && registerMessage.IsVail(id), 
                isUnregisteredOrigin = identity.connection.IsCreated/*NetworkConnection.State.Connected == driver.GetConnectionState(identity.connection)*/ && unregisterMessage.IsVail(id);
            if ((isRegisteredOrigin || isUnregisteredOrigin) && 
                __idNodes.TryGetFirstValue(id, out T node, out var idNodeIterator))
            {
                bool isRegistered = isRegisteredOrigin, isUnregistered = isUnregisteredOrigin;
                int unregisterMessageSize = unregisterMessage.size, registerMessageSize = registerMessage.size, result;
                StatusCode statusCode;
                NodeIdentity nodeIdentity;
                NativeParallelMultiHashMapIterator<T> iterator;
                DataStreamWriter unregisteredWriter = default, registeredWriter = default;
                do
                {
                    if (__nodeIdentities.TryGetFirstValue(node, out nodeIdentity, out iterator))
                    {
                        do
                        {
                            if ((identity.layerMask & (1 << nodeIdentity.layer)) != 0)
                            {
                                //Debug.Log($"[RPC]Unregister {nodeIdentity.value} To {id} In {node}");

                                if (isUnregistered)
                                {
                                    if (unregisteredWriter.IsCreated && unregisteredWriter.Capacity - unregisteredWriter.Length < unregisterMessageSize)
                                    {
                                        result = driver.EndSend(unregisteredWriter);
                                        if (result < 0)
                                        {
                                            statusCode = (StatusCode)result;

                                            __LogUnregisterError(statusCode);
                                        }

                                        unregisteredWriter = default;
                                    }

                                    if (!unregisteredWriter.IsCreated)
                                    {
                                        statusCode = driver.BeginSend(identity.pipeline, identity.connection, out unregisteredWriter);
                                        if (StatusCode.Success != statusCode)
                                        {
                                            unregisteredWriter = default;

                                            __LogUnregisterError(statusCode);

                                            isUnregistered = false;
                                        }
                                    }
                                    
                                    if(isUnregistered)
                                        unregisterMessage.Send(ref unregisteredWriter, nodeIdentity.value, id);
                                    else
                                        unregisterMessage.Dispose(nodeIdentity.value, id);
                                }
                                else if(isUnregisteredOrigin)
                                    unregisterMessage.Dispose(nodeIdentity.value, id);

                                //Debug.Log($"[RPC]Register {nodeIdentity.value} To {id} In {node}");

                                if (isRegistered)
                                {
                                    if (registeredWriter.IsCreated && registeredWriter.Capacity - registeredWriter.Length < registerMessageSize)
                                    {
                                        result = driver.EndSend(registeredWriter);
                                        if (result < 0)
                                        {
                                            statusCode = (StatusCode)result;

                                            __LogRegisterError(statusCode);
                                        }

                                        registeredWriter = default;
                                    }

                                    if (!registeredWriter.IsCreated)
                                    {
                                        statusCode = driver.BeginSend(identity.pipeline, connection, out registeredWriter);
                                        if (StatusCode.Success != statusCode)
                                        {
                                            registeredWriter = default;

                                            __LogRegisterError(statusCode);

                                            isRegistered = false;
                                        }
                                    }

                                    if (isRegistered)
                                        registerMessage.Send(ref registeredWriter, nodeIdentity.value, id);
                                    else
                                        registerMessage.Dispose(nodeIdentity.value, id);
                                }
                                else if(isRegisteredOrigin)
                                    registerMessage.Dispose(nodeIdentity.value, id);
                            }
                        } while (__nodeIdentities.TryGetNextValue(out nodeIdentity, ref iterator));
                    }

                } while (__idNodes.TryGetNextValue(out node, ref idNodeIterator));

                if (unregisteredWriter.IsCreated)
                {
                    result = driver.EndSend(unregisteredWriter);
                    if (result < 0)
                    {
                        statusCode = (StatusCode)result;

                        __LogUnregisterError(statusCode);
                    }
                }

                if (registeredWriter.IsCreated)
                {
                    result = driver.EndSend(registeredWriter);
                    if (result < 0)
                    {
                        statusCode = (StatusCode)result;

                        __LogRegisterError(statusCode);
                    }
                }
            }

            identity.connection = connection;

            __identities[id] = identity;

            return true;
        }

        public bool Register<TDriver, TMessage>(
            uint id, 
            int layer, 
            int layerMask,
            float visibilityDistance,
            ref TDriver driver,
            in NetworkConnection connection,
            in NetworkPipeline pipeline,
            in TMessage message,
            in T node,
            in NativeGraphEx<T> graph,
            ref NativeHashMap<T, float> nodeDistances)
            where TDriver : INetworkDriver
            where TMessage : INetworkRPCMessage
        {
            Identity identity;
            identity.node = node;
            identity.layerMask = layerMask;
            identity.visibilityDistance = visibilityDistance;
            identity.connection = connection;
            identity.pipeline = pipeline;
            if (!__identities.TryAdd(id, identity))
                return false;
            
            nodeDistances.Clear();

            graph.Visit(node, visibilityDistance, ref nodeDistances);

            /*string hehe = "hehe";
            if (__nodeIDs.TryGetFirstValue(node, out var nodeID, out var titerator))
            {
                do
                {
                    bool t = __identities.TryGetValue(nodeID, out var tidentity), b = (tidentity.layerMask & (1 << layer)) != 0;
                    hehe += $":{nodeID}-{t}-{b}-{message.IsVail(nodeID)}-{identity.connection.IsCreated}";

                } while (__nodeIDs.TryGetNextValue(out nodeID, ref titerator));
            }

            UnityEngine.Debug.Log($"[RPCCommand]{id} Register : {node} : {connection}{hehe}");*/

            bool isConnected = identity.connection.IsCreated/*NetworkConnection.State.Connected == driver.GetConnectionState(identity.connection)*/ && message.IsVail(id);
            int messageSize = message.size, result;
            StatusCode statusCode;
            NodeIdentity nodeIdentity;
            T targetNode;
            NativeParallelMultiHashMapIterator<T> iterator;
            DataStreamWriter writer = default;
            foreach (var nodeDistance in nodeDistances)
            {
                targetNode = nodeDistance.Key;

                if (isConnected)
                {
                    if (__nodeIdentities.TryGetFirstValue(targetNode, out nodeIdentity, out iterator))
                    {
                        do
                        {
                            if ((layerMask & (1 << nodeIdentity.layer)) != 0)
                            {
                                if (writer.IsCreated && writer.Capacity - writer.Length < messageSize)
                                {
                                    result = driver.EndSend(writer);
                                    if (result < 0)
                                    {
                                        statusCode = (StatusCode)result;

                                        __LogRegisterError(statusCode);
                                    }

                                    writer = default;
                                }

                                if (!writer.IsCreated)
                                {
                                    statusCode = driver.BeginSend(pipeline, connection, out writer);
                                    if (StatusCode.Success != statusCode)
                                    {
                                        __LogRegisterError(statusCode);

                                        //Debug.Log($"[RPC]Register {nodeIdentity.value} To {id} In {node}");

                                        message.Dispose(nodeIdentity.value, id);
                                        while (__nodeIdentities.TryGetNextValue(out nodeIdentity, ref iterator))
                                        {
                                            if ((layerMask & (1 << nodeIdentity.layer)) != 0)
                                            {
                                                //Debug.Log($"[RPC]Register {nodeIdentity.value} To {id} In {node}");

                                                message.Dispose(nodeIdentity.value, id);
                                            }
                                        }

                                        break;
                                    }
                                }

                                //Debug.Log($"[RPC]Register {nodeIdentity.value} To {id} In {node}");

                                message.Send(ref writer, nodeIdentity.value, id);
                            }
                        } while (__nodeIdentities.TryGetNextValue(out nodeIdentity, ref iterator));
                    }
                }

                __idNodes.Add(id, targetNode); 

                __nodeIDs.Add(targetNode, id);
            }

            if (writer.IsCreated)
            {
                result = driver.EndSend(writer);
                if (result < 0)
                {
                    statusCode = (StatusCode)result;

                    __LogRegisterError(statusCode);
                }
            }

            nodeIdentity.layer = layer;
            nodeIdentity.value = id;
            __nodeIdentities.Add(node, nodeIdentity);

            __RPC(id, layer, node, ref driver, NetworkPipeline.Null, message);

            return true;
        }

        public bool Unregister<TDriver, TMessage>(
            uint id,
            ref TDriver driver,
            in TMessage message)
            where TDriver : INetworkDriver
            where TMessage : INetworkRPCMessage
        {
            if (!__GetIdentity(id, out int layer, out var identity, out var nodeIterator))
                return false;

            //UnityEngine.Debug.Log($"[RPCCommand]{id} Unregister {identity.node} : {identity.connection}");

            __RPC(id, layer, identity.node, ref driver, NetworkPipeline.Null, message);

            __nodeIdentities.Remove(nodeIterator);

            bool isSend = identity.connection.IsCreated/*NetworkConnection.State.Connected == driver.GetConnectionState(identity.connection)*/ && message.IsVail(id);
            if (__idNodes.TryGetFirstValue(id, out T node, out var idNodeIterator))
            {
                uint nodeID;
                int messageSize = message.size, result;
                StatusCode statusCode;
                NodeIdentity nodeIdentity;
                DataStreamWriter writer = default;
                do
                {
                    if (isSend)
                    {
                        if (__nodeIdentities.TryGetFirstValue(node, out nodeIdentity, out nodeIterator))
                        {
                            do
                            {
                                if ((identity.layerMask & (1 << nodeIdentity.layer)) != 0)
                                {
                                    if (writer.IsCreated && writer.Capacity - writer.Length < messageSize)
                                    {
                                        result = driver.EndSend(writer);
                                        if (result < 0)
                                        {
                                            statusCode = (StatusCode)result;

                                            __LogUnregisterError(statusCode);
                                        }

                                        writer = default;
                                    }

                                    if (!writer.IsCreated)
                                    {
                                        statusCode = driver.BeginSend(identity.pipeline, identity.connection, out writer);
                                        if (StatusCode.Success != statusCode)
                                        {
                                            writer = default;

                                            __LogUnregisterError(statusCode);

                                            //Debug.Log($"[RPC]Unregister {nodeIdentity.value} To {id} In {node}");

                                            message.Dispose(nodeIdentity.value, id);
                                            while (__nodeIdentities.TryGetNextValue(out nodeIdentity, ref nodeIterator))
                                            {
                                                if ((identity.layerMask & (1 << nodeIdentity.layer)) != 0)
                                                {
                                                    //Debug.Log($"[RPC]Unregister {nodeIdentity.value} To {id} In {node}");

                                                    message.Dispose(nodeIdentity.value, id);
                                                }
                                            }

                                            break;
                                        }
                                    }

                                    //Debug.Log($"[RPC]Unregister {nodeIdentity.value} To {id} In {node}");

                                    message.Send(ref writer, nodeIdentity.value, id);
                                }
                            } while (__nodeIdentities.TryGetNextValue(out nodeIdentity, ref nodeIterator));
                        }
                    }

                    if (__nodeIDs.TryGetFirstValue(node, out nodeID, out nodeIterator))
                    {
                        do
                        {
                            if (nodeID == id)
                            {
                                __nodeIDs.Remove(nodeIterator);

                                break;
                            }
                        } while (__nodeIDs.TryGetNextValue(out nodeID, ref nodeIterator));
                    }

                } while (__idNodes.TryGetNextValue(out node, ref idNodeIterator));

                if (writer.IsCreated)
                {
                    result = driver.EndSend(writer);
                    if (result < 0)
                    {
                        statusCode = (StatusCode)result;

                        __LogUnregisterError(statusCode);
                    }
                }

                __idNodes.Remove(id);
            }

            __identities.Remove(id);

            return true;
        }

        public bool Move<TDriver, TRegisterMessage, TUnregisterMessage>(
            uint id, 
            int layerMask,
            float visibilityDistance,
            ref TDriver driver,
            in T node,
            in TRegisterMessage registerMessage,
            in TUnregisterMessage unregisterMessage,
            in NativeGraphEx<T> graph,
            ref NativeHashMap<T, float> nodeDistances, 
            ref NativeHashSet<uint> addIDs, 
            ref NativeHashSet<uint> removeIDs)
            where TDriver : INetworkDriver
            where TRegisterMessage : INetworkRPCMessage
            where TUnregisterMessage : INetworkRPCMessage
        {
            if (!__GetIdentity(id, out int layer, out var identity, out var nodeIterator))
                return false;
            
            __nodeIdentities.Remove(nodeIterator);

            removeIDs.Clear();

            Identity targetIdentity;
            if (__nodeIDs.TryGetFirstValue(identity.node, out uint targetID, out nodeIterator))
            {
                do
                {
                    if (__identities.TryGetValue(targetID, out targetIdentity) && (targetIdentity.layerMask & (1 << layer)) != 0)
                            removeIDs.Add(targetID);

                } while (__nodeIDs.TryGetNextValue(out targetID, ref nodeIterator));
            }

            nodeDistances.Clear();

            graph.Visit(node, visibilityDistance, ref nodeDistances);

            /*string hehe = "hehe";
            foreach (var nodeDistance in nodeDistances)
            {
                hehe += $" {nodeDistance.Key}";
            }

            Debug.Log($"[RPCCommand]{id} Move From {identity.node} To {node} : {identity.connection} : {hehe}");*/

            bool isConnected = identity.connection.IsCreated, //NetworkConnection.State.Connected == driver.GetConnectionState(identity.connection),
                isUnregisteredOrigin = isConnected && unregisterMessage.IsVail(id), 
                isUnregistered = isUnregisteredOrigin;
            uint taretID;
            int messageSize, result;
            StatusCode statusCode;
            NodeIdentity nodeIdentity;
            DataStreamWriter writer = default;
            if (__idNodes.TryGetFirstValue(id, out var sourceNode, out var idIterator))
            {
                messageSize = unregisterMessage.size;

                do
                {
                    if(!nodeDistances.ContainsKey(sourceNode))
                    {
                        if (isUnregisteredOrigin &&
                            __nodeIdentities.TryGetFirstValue(sourceNode, out nodeIdentity, out nodeIterator))
                        {
                            do
                            {
                                if ((identity.layerMask & (1 << nodeIdentity.layer)) != 0)
                                {
                                    if (isUnregistered)
                                    {
                                        if (writer.IsCreated && writer.Capacity - writer.Length < messageSize)
                                        {
                                            result = driver.EndSend(writer);
                                            if (result < 0)
                                            {
                                                statusCode = (StatusCode)result;

                                                __LogUnregisterError(statusCode);
                                            }

                                            writer = default;
                                        }

                                        if (!writer.IsCreated)
                                        {
                                            statusCode = driver.BeginSend(identity.pipeline, identity.connection,
                                                out writer);
                                            if (StatusCode.Success != statusCode)
                                            {
                                                //isConnected = false;

                                                isUnregistered = false;

                                                writer = default;

                                                __LogUnregisterError(statusCode);
                                            }
                                        }
                                    }

                                    //Debug.Log($"[RPC]Unregister {nodeIdentity.value} To {id} In {sourceNode}");

                                    if (isUnregistered)
                                        unregisterMessage.Send(ref writer, nodeIdentity.value, id);
                                    else
                                        unregisterMessage.Dispose(nodeIdentity.value, id);
                                }
                            } while (__nodeIdentities.TryGetNextValue(out nodeIdentity, ref nodeIterator));
                        }

                        if(__nodeIDs.TryGetFirstValue(sourceNode, out taretID, out nodeIterator))
                        {
                            do
                            {
                                if(taretID == id)
                                {
                                    __nodeIDs.Remove(nodeIterator);

                                    break;
                                }
                            } while (__nodeIDs.TryGetNextValue(out taretID, ref nodeIterator));
                        }
                    }
                } while (__idNodes.TryGetNextValue(out sourceNode, ref idIterator));
            }

            messageSize = registerMessage.size;

            bool isRegisteredOrigin = isConnected && registerMessage.IsVail(id), 
                isRegistered = isRegisteredOrigin, 
                isContains;
            T destinationNode;
            foreach (var nodeDistance in nodeDistances)
            {
                destinationNode = nodeDistance.Key;

                isContains = false;
                if (__idNodes.TryGetFirstValue(id, out sourceNode, out idIterator))
                {
                    do
                    {
                        if (sourceNode.Equals(destinationNode))
                        {
                            isContains = true;

                            break;
                        }
                    } while (__idNodes.TryGetNextValue(out sourceNode, ref idIterator));
                }

                if (!isContains)
                {
                    __nodeIDs.Add(destinationNode, id);

                    if (isRegisteredOrigin &&
                        __nodeIdentities.TryGetFirstValue(destinationNode, out nodeIdentity, out nodeIterator))
                    {
                        do
                        {
                            if ((layerMask & (1 << nodeIdentity.layer)) != 0)
                            {
                                if (isRegistered)
                                {
                                    if (writer.IsCreated && writer.Capacity - writer.Length < messageSize)
                                    {
                                        result = driver.EndSend(writer);
                                        if (result < 0)
                                        {
                                            statusCode = (StatusCode)result;

                                            __LogRegisterError(statusCode);
                                        }

                                        writer = default;
                                    }

                                    if (!writer.IsCreated)
                                    {
                                        statusCode = driver.BeginSend(identity.pipeline, identity.connection,
                                            out writer);
                                        if (StatusCode.Success != statusCode)
                                        {
                                            //isConnected = false;

                                            isRegistered = false;

                                            writer = default;

                                            __LogRegisterError(statusCode);
                                        }
                                    }
                                }

                                //Debug.Log($"[RPC]Register {nodeIdentity.value} To {id} In {destinationNode}");

                                if (isRegistered)
                                    registerMessage.Send(ref writer, nodeIdentity.value, id);
                                else
                                    registerMessage.Dispose(nodeIdentity.value, id);
                            }
                        } while (__nodeIdentities.TryGetNextValue(out nodeIdentity, ref nodeIterator));
                    }
                }
            }

            if(writer.IsCreated)
            {
                result = driver.EndSend(writer);
                if (result < 0)
                {
                    statusCode = (StatusCode)result;

                    Debug.LogError($"[Move]{statusCode}");
                }
            }

            __idNodes.Remove(id);

            foreach (var nodeDistance in nodeDistances)
                __idNodes.Add(id, nodeDistance.Key);

            addIDs.Clear();
            if (__nodeIDs.TryGetFirstValue(node, out targetID, out nodeIterator))
            {
                do
                {
                    if (__identities.TryGetValue(targetID, out targetIdentity) &&
                    (targetIdentity.layerMask & (1 << layer)) != 0)
                    {
                        if (!removeIDs.Remove(targetID))
                            addIDs.Add(targetID);
                    }
                } while (__nodeIDs.TryGetNextValue(out targetID, ref nodeIterator));
            }

            foreach (uint removeID in removeIDs)
            {
                if (unregisterMessage.IsVail(removeID) && 
                    __identities.TryGetValue(removeID, out targetIdentity) &&
                    targetIdentity.connection.IsCreated)
                {
                    //Debug.Log($"[RPC]Register {id} To {removeID} In {targetIdentity.node}");

                    statusCode = driver.BeginSend(targetIdentity.pipeline, targetIdentity.connection, out writer);
                    if (StatusCode.Success == statusCode)
                    {
                        unregisterMessage.Send(ref writer, id, removeID);

                        result = driver.EndSend(writer);
                        if (result < 0)
                            statusCode = (StatusCode)result;
                    }

                    if (StatusCode.Success != statusCode)
                    {
                        Debug.LogError($"[Move]Unregister {id} for {removeID}: {statusCode}");
                        
                        unregisterMessage.Dispose(id, removeID);
                    }
                }
            }

            foreach (uint addID in addIDs)
            {
                if (registerMessage.IsVail(addID) && 
                    __identities.TryGetValue(addID, out targetIdentity) &&
                    targetIdentity.connection.IsCreated)
                {
                    //Debug.Log($"[RPC]Unregister {id} To {addID} In {targetIdentity.node}");

                    statusCode = driver.BeginSend(targetIdentity.pipeline, targetIdentity.connection, out writer);
                    if (StatusCode.Success == statusCode)
                    {
                        registerMessage.Send(ref writer, id, addID);

                        result = driver.EndSend(writer);
                        if (result < 0)
                            statusCode = (StatusCode)result;
                    }

                    if (StatusCode.Success != statusCode)
                    {
                        //Debug.LogError($"[Move]Register {id} for {addID}: {statusCode}");
                        
                        registerMessage.Dispose(id, addID);
                    }
                }
            }

            nodeIdentity.layer = layer;
            nodeIdentity.value = id;
            __nodeIdentities.Add(node, nodeIdentity);

            identity.node = node;
            identity.visibilityDistance = visibilityDistance;
            __identities[id] = identity;

            return true;
        }

        private bool __RPC<TDriver, TMessage>(
            uint id,
            int layer,
            in T node,
            ref TDriver driver,
            in NetworkPipeline pipeline,
            in TMessage message)
            where TDriver : INetworkDriver
            where TMessage : INetworkRPCMessage
        {
            if (__nodeIDs.TryGetFirstValue(node, out var nodeID, out var iterator))
            {
                int result;
                StatusCode statusCode;
                Identity identity;
                DataStreamWriter writer;
                do
                {
                    if (__identities.TryGetValue(nodeID, out identity) &&
                        (identity.layerMask & (1 << layer)) != 0 && 
                        message.IsVail(nodeID) &&
                        identity.connection.IsCreated)
                    {
                        statusCode = driver.BeginSend(
                            pipeline == NetworkPipeline.Null ? identity.pipeline : pipeline,
                            identity.connection,
                            out writer);

                        if (StatusCode.Success == statusCode)
                        {
                            message.Send(ref writer, id, nodeID);

                            result = driver.EndSend(writer);
                            if (result < 0)
                                statusCode = (StatusCode)result;
                        }
                        else
                            message.Dispose(id, nodeID);

                        if (StatusCode.Success != statusCode)
                            __LogRPCError(id, nodeID, statusCode);
                    }
                } while (__nodeIDs.TryGetNextValue(out nodeID, ref iterator));
            }

            return true;
        }

        private bool __GetIdentity(uint id, out int layer, out Identity identity, out NativeParallelMultiHashMapIterator<T> iterator)
        {
            if (__identities.TryGetValue(id, out identity))
            {
                if (__nodeIdentities.TryGetFirstValue(identity.node, out var nodeIdentity, out iterator))
                {
                    do
                    {
                        if (nodeIdentity.value == id)
                        {
                            layer = nodeIdentity.layer;

                            return true;
                        }
                    } while (__nodeIdentities.TryGetNextValue(out nodeIdentity, ref iterator));
                }
            }

            layer = 0;
            iterator = default;

            return false;
        }

        private void __LogRegisterError(StatusCode statusCode)
        {
            UnityEngine.Debug.LogError($"Register: {(int)statusCode}");
        }

        private void __LogUnregisterError(StatusCode statusCode)
        {
            UnityEngine.Debug.LogError($"Unregister: {(int)statusCode}");
        }

        private void __LogRPCError(uint sourceID, uint destinationID, StatusCode statusCode)
        {
            UnityEngine.Debug.LogError($"RPC {sourceID} for {destinationID}: {(int)statusCode}");
        }
    }

    public struct NetworkRPCCommander
    {
        private enum CommandType
        {
            Register,
            Unregister
        }

        public enum InitType
        {
            Normal,
            New
        }

        public enum ActiveEventType
        {
            Enable,
            Disable
        }

        public struct ActiveEvent
        {
            public ActiveEventType type;
            public uint id;
        }

        public struct InitEvent
        {
            public InitType type;
            public uint id;
        }

        private struct CommandIndex
        {
            public CommandType type;
            public int value;
        }

        private struct RPCCommand : IComparable<RPCCommand>
        {
            public int type;

            public uint id;

            public NetworkPipeline pipeline;

            public NetworkBufferSegment bufferSegment;
            public NetworkBufferSegment additionalIDs;
            public NetworkBufferSegment maskIDs;

            public int CompareTo(RPCCommand other)
            {
                int result = pipeline.GetHashCode().CompareTo(other.pipeline.GetHashCode());
                if(result == 0)
                    return bufferSegment.byteOffset.CompareTo(other.bufferSegment.byteOffset);

                return result;
            }
        }

        private struct RegisterCommand
        {
            public int type;
            public int node;
            public int layer;
            public int layerMask;
            public float visibilityDistance;
            public NetworkConnection connection;

            public NetworkPipeline pipeline;

            public NetworkBufferSegment bufferSegment;
        }

        private struct UnregisterCommand
        {
            public NetworkPipeline pipeline;

            public NetworkBufferSegment bufferSegment;
        }

        private struct ReconnectCommand
        {
            public int type;
            public NetworkConnection connection;

            public NetworkPipeline pipeline;

            public NetworkBufferSegment registerBufferSegment;
            public NetworkBufferSegment unregisterBufferSegment;
        }

        private struct MoveCommand
        {
            public int node;
            public int layerMask;
            public float visibilityDistance;

            public NetworkPipeline pipeline;

            public NetworkBufferSegment registerBufferSegment;
            public NetworkBufferSegment unregisterBufferSegment;
        }

        private struct InitCommand
        {
            public InitType type;
            public uint id;

            public NetworkPipeline pipeline;

            public NetworkBufferSegment bufferSegment;
        }

        private struct Version
        {
            public uint id;
            public uint value;

#if DEBUG
            public bool isActive;
#endif

            public static uint Get(
                in NativeParallelMultiHashMap<uint, Version> versions, 
                uint sourceID, 
                uint destinationID, 
                out NativeParallelMultiHashMapIterator<uint> iterator
#if DEBUG
                , out bool isActive
#endif
                )
            {
#if DEBUG
                isActive = false;
#endif
                if (versions.TryGetFirstValue(destinationID, out var version, out iterator))
                {
                    do
                    {
                        if (version.id == sourceID)
                        {
#if DEBUG
                            isActive = version.isActive;
#endif
                            return version.value;
                        }
                    } while (versions.TryGetNextValue(out version, ref iterator));
                }

                return 0;
            }
        }

        private struct RPCMessage : INetworkRPCMessage
        {
            public int type;
            //[ReadOnly]
            public StreamCompressionModel model;
            //[ReadOnly]
            public NativeArray<uint> maskIDs;
            //[ReadOnly]
            public NativeArray<byte> bytes;
            //[ReadOnly]
            public NativeParallelMultiHashMap<uint, Version> versions;

            public int size => (UnsafeUtility.SizeOf<uint>() << 2) + bytes.Length;

            public bool IsVail(uint id) => !maskIDs.IsCreated || maskIDs.IndexOf(id) == -1;

            public void Send(ref DataStreamWriter writer, uint sourceID, uint destinationID)
            {
                if (type < 0)
                {
                    writer.WritePackedUInt((uint)NetworkMessageType.RPC, model);
                    writer.WritePackedUInt(sourceID, model);
                    uint version = Version.Get(
                        versions,
                        sourceID,
                        destinationID,
                        out _
#if DEBUG
                        , out bool isActive
#endif
                        );

#if DEBUG
                    if (!isActive)
                        Debug.LogError($"[RPC]{sourceID} RPC to {destinationID} with a inactive message.");
#endif

                    UnityEngine.Assertions.Assert.AreNotEqual(0, version);

                    writer.WritePackedUInt(version, model);
                    writer.WritePackedUInt((uint)bytes.Length, model);
                }
                else
                {
                    writer.WritePackedUInt((uint)type, model);
                    writer.WriteUShort((ushort)bytes.Length);
                }

                writer.WriteBytes(bytes);
            }
            
            public void Dispose(uint sourceID, uint destinationID)
            {
                if (type < 0)
                {
                    uint version = Version.Get(
                        versions,
                        sourceID,
                        destinationID,
                        out _
#if DEBUG
                        , out bool isActive
#endif
                    );

#if DEBUG
                    if (!isActive)
                        Debug.LogError($"[RPC]{sourceID} RPC to {destinationID} with a inactive message.");
#endif

                    UnityEngine.Assertions.Assert.AreNotEqual(0, version);
                }
            }
        }

        private struct RegisterMessage : INetworkRPCMessage
        {
            //[ReadOnly]
            public StreamCompressionModel model;
            //[ReadOnly]
            public NativeHashMap<uint, int> types;
            //[ReadOnly]
            public NativeArray<byte> bytes;
            public NativeParallelMultiHashMap<uint, Version> versions;

            public int size => bytes.Length  + UnsafeUtility.SizeOf<uint>() * 5;

            public bool IsVail(uint id) => bytes.IsCreated;

            public void Send(ref DataStreamWriter writer, uint sourceID, uint destinationID)
            {
                writer.WritePackedUInt((uint)NetworkMessageType.Register, model);
                writer.WritePackedUInt(sourceID, model);

                Version version;
                version.id = sourceID;

                version.value = Version.Get(
                    versions, 
                    sourceID, 
                    destinationID, 
                    out var iterator
#if DEBUG
                    , out version.isActive
#endif
                    ) + 1;
                writer.WritePackedUInt(version.value, model);

#if DEBUG
                //Debug.Log($"[RPC]Register {sourceID} To {destinationID}.");
                
                if (version.isActive)
                    Debug.LogError($"[RPC]Register {sourceID} To {destinationID} with an active version.");
                
                //UnityEngine.Assertions.Assert.IsFalse(version.isActive);

                version.isActive = true;
#endif

                if (version.value == 1)
                    versions.Add(destinationID, version);
                else
                    versions.SetValue(version, iterator);

                int type = types[sourceID];
                uint identity = (uint)(type << 1) | (sourceID == destinationID ? 1u : 0u);
                writer.WritePackedUInt(identity, model);
                writer.WritePackedUInt((uint)bytes.Length, model);
                writer.WriteBytes(bytes);
            }
            
            public void Dispose(uint sourceID, uint destinationID)
            {
                Version version;
                version.id = sourceID;

                version.value = Version.Get(
                    versions, 
                    sourceID, 
                    destinationID, 
                    out var iterator
#if DEBUG
                    , out version.isActive
#endif
                ) + 1;
                
#if DEBUG
                //Debug.Log($"[RPC]Register {sourceID} To {destinationID}.");

                if (version.isActive)
                    Debug.LogError($"[RPC]Register {sourceID} To {destinationID} with an active version.");

                //UnityEngine.Assertions.Assert.IsFalse(version.isActive);
                
                version.isActive = true;
#endif
                if (version.value == 1)
                    versions.Add(destinationID, version);
                else
                    versions.SetValue(version, iterator);
            }
        }

        private struct UnregisterMessage : INetworkRPCMessage
        {
            //[ReadOnly]
            public StreamCompressionModel model;
            //[ReadOnly]
            public NativeArray<byte> bytes;

#if DEBUG
            public NativeParallelMultiHashMap<uint, Version> versions;
#endif

            public int size => bytes.Length + UnsafeUtility.SizeOf<uint>() * 3;

            public bool IsVail(uint id) => bytes.IsCreated;

            public void Send(ref DataStreamWriter writer, uint sourceID, uint destinationID)
            {
#if DEBUG
                Version version;
                version.id = sourceID;

                version.value = Version.Get(
                    versions,
                    sourceID,
                    destinationID,
                    out var iterator, 
                    out bool isActive);

                UnityEngine.Assertions.Assert.AreNotEqual(0, version.value);
                //UnityEngine.Assertions.Assert.IsTrue(isActive);
                
                //Debug.Log($"[RPC]Unregister {sourceID} To {destinationID}.");
                
                if (!isActive)
                    Debug.LogError($"[RPC]Unregister {sourceID} To {destinationID} with an active version.");

                version.isActive = false;

                versions.SetValue(version, iterator);
#endif

                writer.WritePackedUInt((uint)NetworkMessageType.Unregister, model);
                writer.WritePackedUInt(sourceID, model);
                writer.WritePackedUInt((uint)bytes.Length, model);
                writer.WriteBytes(bytes);
            }
            
            public void Dispose(uint sourceID, uint destinationID)
            {
#if DEBUG
                Version version;
                version.id = sourceID;

                version.value = Version.Get(
                    versions,
                    sourceID,
                    destinationID,
                    out var iterator, 
                    out bool isActive);
                
                //Debug.Log($"[RPC]Unregister {sourceID} To {destinationID}.");
                
                if(!isActive)
                    Debug.LogError($"[RPC]Unregister {sourceID} To {destinationID} with an inactive version.");
                
                UnityEngine.Assertions.Assert.AreNotEqual(0, version.value);
                //UnityEngine.Assertions.Assert.IsTrue(isActive);

                version.isActive = false;

                versions.SetValue(version, iterator);
#endif
            }
        }

        [BurstCompile]
        private struct Command : IJob
        {
            private struct Driver : INetworkDriver
            {
                private NativeHashMap<NetworkConnection, uint> __ids;
                private NetworkDriverBuffer<uint> __instance;

                public Driver(
                    in NativeHashMap<NetworkConnection, uint> ids, 
                    ref NetworkDriver.Concurrent instance,
                    ref NativeList<NetworkBuffer> bufferPool,
                    ref NativeList<NetworkDriverBuffer<uint>.Command> commands,
                    ref NativeParallelMultiHashMap<uint, NetworkBufferInstance> bufferInstances)
                {
                    __ids = ids;

                    __instance = new NetworkDriverBuffer<uint>(
                        ref instance, 
                        ref bufferPool, 
                        ref commands,
                        ref bufferInstances);
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
                    if (!__ids.TryGetValue(connection, out uint id))
                    {
                        writer = default;

                        return StatusCode.NetworkIdMismatch;
                    }

                    return __instance.BeginSend(pipeline, id, out writer, requiredPayloadSize);
                }

                public int EndSend(DataStreamWriter writer)
                {
                    return __instance.EndSend(writer);
                }

                public void AbortSend(DataStreamWriter writer)
                {
                    __instance.AbortSend(writer);
                }
            }

            [ReadOnly]
            public NativeHashMap<NetworkConnection, uint> ids;

            [ReadOnly]
            public NativeGraphEx<int> graph;

            [ReadOnly]
            public NativeBuffer buffer;

            [ReadOnly]
            public NetworkDriver.Concurrent driver;

            public StreamCompressionModel model;

            public NativeArray<int> rpcBufferCount;

            public NativeList<uint> rpcIDs;

            public NativeList<ActiveEvent> activeEvents;

            public NativeList<RegisterCommand> registerCommands;
            public NativeList<UnregisterCommand> unregisterCommands;

            public NativeList<NetworkDriverBuffer<uint>.Command> bufferCommands;
            public NativeList<NetworkBuffer> bufferPool;

            public NativeHashMap<uint, CommandIndex> commandIndices;
            public NativeHashMap<uint, MoveCommand> moveCommands;
            public NativeHashMap<uint, ReconnectCommand> reconnectCommands;

            public NativeHashMap<uint, int> types;

            public NativeHashMap<int, float> nodeDistances;

            public NativeParallelMultiHashMap<uint, Version> versions;

            public NativeParallelMultiHashMap<uint, NetworkBufferInstance> bufferInstanceInputs;

            public NativeParallelMultiHashMap<uint, NetworkBufferInstance> bufferInstanceOutputs;

            public NativeParallelMultiHashMap<uint, InitEvent> initEvents;

            public NativeParallelMultiHashMap<uint, uint> idsToInit;

            public NativeHashSet<uint> idsToActive;

            public NativeHashSet<uint> addIDs;

            public NativeHashSet<uint> removeIDs;

            public NetworkRPCManager<int> manager;

            public void BeginRPC()
            {
                bufferCommands.Clear();

                UnityEngine.Assertions.Assert.IsFalse(rpcBufferCount[0] > bufferPool.Length);

                bufferPool.ResizeUninitialized(rpcBufferCount[0]);

                foreach (var bufferInstance in bufferInstanceInputs)
                    bufferPool.Add(bufferInstance.Value.buffer);

                bufferInstanceInputs.Clear();

                foreach (var bufferInstance in bufferInstanceOutputs)
                    bufferInstanceInputs.Add(bufferInstance.Key, bufferInstance.Value);

                bufferInstanceOutputs.Clear();
            }

            public void EndRPC()
            {
                rpcIDs.Clear();
                using (var ids = bufferInstanceInputs.GetKeyArray(Allocator.Temp))
                {
                    int count = ids.ConvertToUniqueArray();
                    rpcIDs.AddRange(ids.GetSubArray(0, count));
                }

                rpcBufferCount[0] = bufferPool.Length;

#if DEBUG
                foreach (var bufferCommand in bufferCommands)
                    UnityEngine.Assertions.Assert.IsFalse(bufferCommand.isActive);
#endif
            }

            public void Execute()
            {
                BeginRPC();

                activeEvents.Clear();
                //initEvents.Clear();

                var driver = new Driver(ids, ref this.driver, ref bufferPool, ref bufferCommands, ref bufferInstanceInputs);

                RegisterMessage registerMessage;
                registerMessage.model = model;
                registerMessage.types = types;
                registerMessage.versions = versions;

                UnregisterMessage unregisterMessage;
                unregisterMessage.model = model;

#if DEBUG
                unregisterMessage.versions = versions;
#endif

                UnsafeHashSet<uint> nodeIDs = default, idsToBeChanged = default;
                //NativeParallelHashMap<int, float> nodeDistances = default;
                NativeParallelMultiHashMapIterator<uint> iterator;
                CommandIndex commandIndex;
                int layerMask;
                uint id;
                foreach (var pair in commandIndices)
                {
                    commandIndex = pair.Value;
                    switch (commandIndex.type)
                    {
                        case CommandType.Register:
                            var registerCommand = registerCommands[commandIndex.value];

                            id = pair.Key;
                            registerMessage.bytes = registerCommand.bufferSegment.GetArray(buffer);

                            if (!nodeDistances.IsCreated)
                                nodeDistances = new NativeHashMap<int, float>(1, Allocator.Temp);

                            types[id] = registerCommand.type;

                            if (manager.Register(
                                id,
                                registerCommand.layer,
                                registerCommand.layerMask,
                                registerCommand.visibilityDistance,
                                ref driver,
                                registerCommand.connection,
                                registerCommand.pipeline,
                                registerMessage,
                                registerCommand.node,
                                graph,
                                ref nodeDistances))
                            {
                                if (NetworkConnection.State.Connected == driver.GetConnectionState(registerCommand.connection) && registerCommand.layerMask != 0)
                                {
                                    foreach (var nodeDistance in nodeDistances)
                                    {
                                        foreach (var nodeIdentity in manager.GetNodeIdentities(nodeDistance.Key))
                                        {
                                            if (((1 << nodeIdentity.layer) & registerCommand.layerMask) != 0)
                                            {
                                                __Init(InitType.New, id, nodeIdentity.value);

                                                idsToInit.Add(id, nodeIdentity.value);

                                                __MaskBeChanged(nodeIdentity.value, ref idsToBeChanged);
                                            }
                                        }
                                    }
                                }

                                bool isChanged = false;
                                uint targetID;
                                InitType initType;
                                foreach (uint nodeID in manager.GetNodeIDs(registerCommand.node))
                                {
                                    if (NetworkConnection.State.Connected == driver.GetConnectionState(manager.GetConnection(nodeID, registerCommand.layer)))
                                    {
                                        initType = InitType.New;
                                        if (idsToInit.TryGetFirstValue(nodeID, out targetID, out iterator))
                                        {
                                            do
                                            {
                                                if (targetID == id)
                                                {
                                                    initType = InitType.Normal;

                                                    break;
                                                }
                                            } while (idsToInit.TryGetNextValue(out targetID, ref iterator));
                                        }

                                        __Init(initType, nodeID, id);

                                        if (initType == InitType.New)
                                            idsToInit.Add(nodeID, id);

                                        isChanged = true;
                                    }
                                }

                                if (isChanged)
                                    __MaskBeChanged(id, ref idsToBeChanged);
                            }
                            else
                                types.Remove(id);
                            break;
                        case CommandType.Unregister:
                            id = pair.Key;

                            idsToInit.Remove(id);

                            //���ﲻӦ���ж����ӣ���Ϊ���ܴ�ʱ�Ѿ��Ͽ���
                            //if (NetworkConnection.State.Connected == driver.GetConnectionState(manager.GetConnection(id)))
                            {
                                layerMask = manager.GetLayerMask(id);
                                if (layerMask != 0)
                                {
                                    foreach (int idNode in manager.GetIDNodes(id))
                                    {
                                        foreach (var nodeIdentity in manager.GetNodeIdentities(idNode))
                                        {
                                            if (((1 << nodeIdentity.layer) & layerMask) != 0)
                                                __MaskBeChanged(nodeIdentity.value, ref idsToBeChanged);
                                        }
                                    }
                                }
                            }

                            if (manager.TryGetNode(id, out int node))
                            {
                                bool isChanged = false;
                                uint targetID;
                                foreach (uint nodeID in manager.GetNodeIDs(node))
                                {
                                    if (idsToInit.TryGetFirstValue(nodeID, out targetID, out iterator))
                                    {
                                        do
                                        {
                                            if (targetID == id)
                                            {
                                                idsToInit.Remove(iterator);

                                                isChanged = true;

                                                break;
                                            }
                                        } while (idsToInit.TryGetNextValue(out targetID, ref iterator));
                                    }
                                }

                                if(isChanged)
                                    __MaskBeChanged(id, ref idsToBeChanged);
                            }

                            var unregisterCommand = unregisterCommands[commandIndex.value];

                            unregisterMessage.bytes = unregisterCommand.bufferSegment.GetArray(buffer);

/*#if DEBUG
                            if (nodeIDs.IsCreated)
                                nodeIDs.Clear();

                            layerMask = manager.GetLayerMask(id);
                            if (layerMask != 0)
                            {
                                foreach (int idNode in manager.GetIDNodes(id))
                                {
                                    foreach (var nodeIdentity in manager.GetNodeIdentities(idNode))
                                    {
                                        if (((1 << nodeIdentity.layer) & layerMask) != 0)
                                        {
                                            if (!nodeIDs.IsCreated)
                                                nodeIDs = new UnsafeHashSet<uint>(1, Allocator.Temp);

                                            nodeIDs.Add(nodeIdentity.value);
                                        }
                                    }
                                }
                            }
#endif*/
                            if (manager.Unregister(
                                id,
                                ref driver,
                                unregisterMessage))
                            {
                                types.Remove(id);

/*#if DEBUG
                                foreach(var nodeID in nodeIDs)
                                {
                                    Version version;
                                    version.id = nodeID;

                                    version.value = Version.Get(
                                        versions,
                                        nodeID,
                                        id,
                                        out iterator,
                                        out version.isActive);

                                    if (version.value != 0 && version.isActive)
                                    {
                                        version.isActive = false;

                                        versions.SetValue(version, iterator);
                                    }
                                }
#endif*/
                            }

                            break;
                    }
                }

                registerCommands.Clear();
                unregisterCommands.Clear();
                commandIndices.Clear();

                bool isConnected, isContans;
                MoveCommand moveCommand;
                //NativeParallelHashSet<uint> addIDs = default, removeIDs = default;
                foreach(var pair in moveCommands)
                {
                    moveCommand = pair.Value;

                    id = pair.Key;

                    isConnected = NetworkConnection.State.Connected == driver.GetConnectionState(manager.GetConnection(id));
                    if (isConnected)
                    {
                        if (nodeIDs.IsCreated)
                            nodeIDs.Clear();

                        layerMask = manager.GetLayerMask(id);
                        if (layerMask != 0)
                        {
                            foreach (int node in manager.GetIDNodes(id))
                            {
                                foreach (var nodeIdentity in manager.GetNodeIdentities(node))
                                {
                                    if (((1 << nodeIdentity.layer) & layerMask) != 0)
                                    {
                                        if (!nodeIDs.IsCreated)
                                            nodeIDs = new UnsafeHashSet<uint>(1, Allocator.Temp);

                                        nodeIDs.Add(nodeIdentity.value);
                                    }
                                }
                            }
                        }
                    }

                    registerMessage.bytes = moveCommand.registerBufferSegment.GetArray(buffer);
                    unregisterMessage.bytes = moveCommand.unregisterBufferSegment.GetArray(buffer);

                    if (!nodeDistances.IsCreated)
                        nodeDistances = new NativeHashMap<int, float>(1, Allocator.Temp);

                    if (!addIDs.IsCreated)
                        addIDs = new NativeHashSet<uint>(1, Allocator.Temp);

                    if (!removeIDs.IsCreated)
                        removeIDs = new NativeHashSet<uint>(1, Allocator.Temp);

                    if (manager.Move(
                        id,
                        moveCommand.layerMask,
                        moveCommand.visibilityDistance,
                        ref driver,
                        moveCommand.node,
                        registerMessage,
                        unregisterMessage,
                        graph,
                        ref nodeDistances,
                        ref addIDs,
                        ref removeIDs))
                    {
                        InitType initType;
                        uint targetID;
                        if (isConnected)
                        {
                            foreach (var nodeID in nodeIDs)
                            {
                                isContans = false;
                                foreach (var nodeDistance in nodeDistances)
                                {
                                    foreach (var nodeIdentity in manager.GetNodeIdentities(nodeDistance.Key))
                                    {
                                        if (nodeIdentity.value == nodeID && ((1 << nodeIdentity.layer) & moveCommand.layerMask) != 0)
                                        {
                                            isContans = true;

                                            break;
                                        }
                                    }

                                    if (isContans)
                                        break;
                                }

                                if (isContans)
                                    continue;

                                __MaskBeChanged(nodeID, ref idsToBeChanged);
                            }

                            foreach (var nodeDistance in nodeDistances)
                            {
                                foreach (var nodeIdentity in manager.GetNodeIdentities(nodeDistance.Key))
                                {
                                    if (((1 << nodeIdentity.layer) & moveCommand.layerMask) != 0 && !nodeIDs.Contains(nodeIdentity.value))
                                    {
                                        initType = InitType.New;
                                        if (idsToInit.TryGetFirstValue(id, out targetID, out iterator))
                                        {
                                            do
                                            {
                                                if (targetID == nodeIdentity.value)
                                                {
                                                    initType = InitType.Normal;

                                                    break;
                                                }
                                            } while (idsToInit.TryGetNextValue(out targetID, ref iterator));
                                        }

                                        __Init(initType, id, nodeIdentity.value);

                                        if (initType == InitType.New)
                                            idsToInit.Add(id, nodeIdentity.value);

                                        __MaskBeChanged(nodeIdentity.value, ref idsToBeChanged);
                                    }
                                }
                            }
                        }

                        bool isChanged = false;
                        foreach (uint addID in addIDs)
                        {
                            if (NetworkConnection.State.Connected == driver.GetConnectionState(manager.GetConnection(addID)))
                            {
                                initType = InitType.New;
                                if (idsToInit.TryGetFirstValue(addID, out targetID, out iterator))
                                {
                                    do
                                    {
                                        if (targetID == id)
                                        {
                                            initType = InitType.Normal;

                                            break;
                                        }
                                    } while (idsToInit.TryGetNextValue(out targetID, ref iterator));
                                }

                                __Init(initType, addID, id);

                                if (initType == InitType.New)
                                    idsToInit.Add(addID, id);

                                isChanged = true;
                            }
                        }

                        if(!isChanged)
                        {
                            foreach (uint removeID in removeIDs)
                            {
                                if (NetworkConnection.State.Connected == driver.GetConnectionState(manager.GetConnection(removeID)))
                                {
                                    isChanged = true;

                                    break;
                                }
                            }
                        }

                        if(isChanged)
                            __MaskBeChanged(id, ref idsToBeChanged);
                    }
                }

                moveCommands.Clear();

                /*if (nodeDistances.IsCreated)
                    nodeDistances.Dispose();

                if (addIDs.IsCreated)
                    addIDs.Dispose();

                if (removeIDs.IsCreated)
                    removeIDs.Dispose();*/

                //NetworkConnection connection;
                ReconnectCommand reconnectCommand;
                foreach (var pair in reconnectCommands)
                {
                    reconnectCommand = pair.Value;

                    id = pair.Key;

                    registerMessage.bytes = reconnectCommand.registerBufferSegment.GetArray(buffer);
                    unregisterMessage.bytes = reconnectCommand.unregisterBufferSegment.GetArray(buffer);
                     
/*#if DEBUG
                    connection = manager.GetConnection(id);
                    if ((NetworkConnection.State.Connected != driver.GetConnectionState(connection) || !unregisterMessage.IsVail(id)) &&
                        (NetworkConnection.State.Connected == driver.GetConnectionState(reconnectCommand.connection) && registerMessage.IsVail(id)))
                    {
                        layerMask = manager.GetLayerMask(id);
                        if (layerMask != 0)
                        {
                            foreach (int node in manager.GetIDNodes(id))
                            {
                                foreach (var nodeIdentity in manager.GetNodeIdentities(node))
                                {
                                    if (((1 << nodeIdentity.layer) & layerMask) != 0)
                                    {
                                        Version version;
                                        version.id = nodeIdentity.value;
                                        
                                        version.value = Version.Get(
                                            versions,
                                            nodeIdentity.value,
                                            id,
                                            out iterator,
                                            out bool isActive);

                                        if (version.value != 0)
                                        {
                                            version.isActive = false;

                                            versions.SetValue(version, iterator);
                                        }
                                    }
                                }
                            }
                        }
                    }
#endif*/

                    if (manager.Reconnect(
                        id,
                        ref driver,
                        reconnectCommand.connection,
                        registerMessage,
                        unregisterMessage))
                    {
                        idsToInit.Remove(id);

                        isConnected = NetworkConnection.State.Connected == driver.GetConnectionState(reconnectCommand.connection);

                        layerMask = manager.GetLayerMask(id);
                        if (layerMask != 0)
                        {
                            foreach (int node in manager.GetIDNodes(id))
                            {
                                foreach (var nodeIdentity in manager.GetNodeIdentities(node))
                                {
                                    if (((1 << nodeIdentity.layer) & layerMask) != 0)
                                    {
                                        if (isConnected)
                                        {
                                            __Init(InitType.New, id, nodeIdentity.value);

                                            idsToInit.Add(id, nodeIdentity.value);
                                        }

                                        __MaskBeChanged(nodeIdentity.value, ref idsToBeChanged);
                                    }
                                }
                            }
                        }

                        types[id] = reconnectCommand.type;
                    }
                }

                reconnectCommands.Clear();

                if (idsToBeChanged.IsCreated)
                {
                    ActiveEvent activeEvent;
                    foreach(var idToBeChanged in idsToBeChanged)
                    {
                        if(manager.IsActive(idToBeChanged, driver))
                        {
                            if(idsToActive.Add(idToBeChanged))
                            {
                                activeEvent.type = ActiveEventType.Enable;
                                activeEvent.id = idToBeChanged;

                                activeEvents.Add(activeEvent);
                            }
                        }
                        else if(idsToActive.Remove(idToBeChanged))
                        {
                            activeEvent.type = ActiveEventType.Disable;
                            activeEvent.id = idToBeChanged;

                            activeEvents.Add(activeEvent);
                        }
                    }

                    idsToBeChanged.Dispose();
                }

                EndRPC();
            }

            private bool __Init(InitType type, uint sourceID, uint destinationID)
            {
                if(initEvents.TryGetFirstValue(sourceID, out var initEvent, out var iterator))
                {
                    do
                    {
                        if (initEvent.id == destinationID)
                            return false;
                    } while (initEvents.TryGetNextValue(out initEvent, ref iterator));
                }

                initEvent.type = type;
                initEvent.id = destinationID;
                initEvents.Add(sourceID, initEvent);

                return true;
            }

            private void __MaskBeChanged(uint id, ref UnsafeHashSet<uint> idsToBeChanged)
            {
                if (!idsToBeChanged.IsCreated)
                    idsToBeChanged = new UnsafeHashSet<uint>(1, Allocator.Temp);

                idsToBeChanged.Add(id);
            }
        }

        [BurstCompile]
        private struct CollectRPC : IJob
        {
            [ReadOnly]
            public NetworkDriver.Concurrent driver;

            [ReadOnly]
            public NativeParallelMultiHashMap<uint, InitCommand> initCommands;

            [ReadOnly]
            public NativeBuffer buffer;

            public NetworkRPCManager<int>.ReadOnly manager;

            public NativeHashMap<uint, int> minCommandByteOffsets;

            public NativeParallelMultiHashMap<uint, NetworkBufferInstance> bufferInstanceResults;

            public NativeParallelMultiHashMap<uint, RPCCommand> destinations;

            public NativeList<RPCCommand> sources;

            public NativeList<uint> rpcIDs;

            public void Execute()
            {
                destinations.Clear();

                foreach (var command in sources)
                {
                    /*if (minCommandByteOffsets.TryGetValue(command.id, out minCommandByteOffset) && minCommandByteOffset > command.bufferSegment.byteOffset)
                        continue;*/

                    switch (command.type)
                    {
                        case (int)NetworkRPCType.None:
                            break;
                        case (int)NetworkRPCType.Normal:
                            manager.RPC(command.id, command, ref destinations);
                            break;
                        case (int)NetworkRPCType.SendSelfOnly:
                            if (manager.GetConnection(command.id).IsCreated)
                                destinations.Add(command.id, command);
                            break;
                    }

                    var additionalIDs = command.additionalIDs.GetArray<uint, NativeBuffer>(buffer);
                    foreach (uint additionalID in additionalIDs)
                    {
                        if (manager.GetConnection(additionalID, command.id).IsCreated)
                            destinations.Add(additionalID, command);
                    }
                }

                sources.Clear();

                //rpcIDs.Clear();

                int numRPCIDs = rpcIDs.Length;
                if (!destinations.IsEmpty)
                {
                    for (int i = 0; i < numRPCIDs; ++i)
                    {
                        if (destinations.ContainsKey(rpcIDs[i]))
                        {
                            rpcIDs.RemoveAtSwapBack(i--);

                            --numRPCIDs;
                        }
                    }

                    using (var ids = destinations.GetKeyArray(Allocator.Temp))
                    {
                        int count = ids.ConvertToUniqueArray();
                        rpcIDs.AddRange(ids.GetSubArray(0, count));

                        int minCommandByteOffset;
                        RPCCommand command;
                        NativeParallelMultiHashMapIterator<uint> iterator;
                        foreach (var id in ids)
                        {
                            if (minCommandByteOffsets.TryGetValue(id, out minCommandByteOffset))
                            {
                                if (destinations.TryGetFirstValue(id, out command, out iterator))
                                {
                                    do
                                    {
                                        if(minCommandByteOffset > command.bufferSegment.byteOffset)
                                            destinations.Remove(iterator);
                                        
                                    } while (destinations.TryGetNextValue(out command, ref iterator));
                                }
                            }
                        }
                    }
                }

                minCommandByteOffsets.Clear();

                if (!initCommands.IsEmpty)
                {
                    numRPCIDs = rpcIDs.Length;
                    for (int i = 0; i < numRPCIDs; ++i)
                    {
                        if (initCommands.ContainsKey(rpcIDs[i]))
                        {
                            rpcIDs.RemoveAtSwapBack(i--);

                            --numRPCIDs;
                        }
                    }

                    using (var ids = initCommands.GetKeyArray(Allocator.Temp))
                    {
                        int count = ids.ConvertToUniqueArray();
                        rpcIDs.AddRange(ids.GetSubArray(0, count));
                    }
                }

                UnityEngine.Assertions.Assert.AreEqual(0, bufferInstanceResults.Count());
                bufferInstanceResults.Capacity = math.max(bufferInstanceResults.Capacity, rpcIDs.Length * driver.PipelineCount());
            }
        }

        [BurstCompile]
        private struct CommandParallel : IJobParallelForDefer
        {
            public StreamCompressionModel model;

            public NetworkRPCManager<int>.ReadOnly manager;

            public NetworkDriver.Concurrent driver;

            [ReadOnly]
            public NativeBuffer buffer;

            [ReadOnly]
            public NativeArray<NetworkBuffer> rpcBufferPool;

            [ReadOnly]
            public NativeArray<uint> rpcIDs;

            [ReadOnly]
            public NativeParallelMultiHashMap<uint, Version> versions;

            [ReadOnly]
            public NativeParallelMultiHashMap<uint, InitCommand> initCommands;

            [ReadOnly]
            public NativeParallelMultiHashMap<uint, InitEvent> initEvents;

            [ReadOnly]
            public NativeParallelMultiHashMap<uint, RPCCommand> commands;

            [ReadOnly]
            public NativeParallelMultiHashMap<uint, NetworkBufferInstance> rpcBufferInstanceInputs;

            public NativeParallelMultiHashMap<uint, NetworkBufferInstance>.ParallelWriter rpcBufferInstanceOutputs;

            [NativeDisableParallelForRestriction]
            public NativeArray<int> rpcBufferPoolCount;

            public void Execute(int index)
            {
                uint id = rpcIDs[index];
                var connection = manager.GetConnection(id);
                if (!connection.IsCreated)
                    return;

                var driver = new NetworkDriverWrapper(ref this.driver, ref rpcBufferPoolCount, rpcBufferPool);

                UnsafeHashMap<NetworkPipeline, DataStreamWriter> writers = default;
                __CommandMain(id, connection, ref driver, ref writers);
                __CommandInit(id, connection, ref driver, ref writers);
                __CommandRPC(id, connection, ref driver, ref writers);

                if (writers.IsCreated)
                {
                    int result;
                    foreach (var writer in writers)
                    {
                        result = driver.EndSend(writer.Value);
                        if (result < 0)
                            __LogError((StatusCode)result);
                    }

                    writers.Dispose();
                }

                driver.Apply(id, ref rpcBufferInstanceOutputs);

                driver.Dispose();
            }

            private void __CommandMain(uint id, in NetworkConnection connection, ref NetworkDriverWrapper driver, ref UnsafeHashMap<NetworkPipeline, DataStreamWriter> writers)
            {
                if (rpcBufferInstanceInputs.TryGetFirstValue(id, out var bufferInstance, out var iterator))
                {
                    if (!writers.IsCreated)
                        writers = new UnsafeHashMap<NetworkPipeline, DataStreamWriter>(1, Allocator.Temp);

                    DataStreamWriter writer = default;
                    do
                    {
                        if (!writers.TryGetValue(bufferInstance.pipeline, out writer))
                            writer = default;

                        bufferInstance.buffer.Apply(connection, bufferInstance.pipeline, ref driver, ref writer);

                        if (writer.IsCreated)
                            writers[bufferInstance.pipeline] = writer;
                        else
                            writers.Remove(bufferInstance.pipeline);

                    } while (rpcBufferInstanceInputs.TryGetNextValue(out bufferInstance, ref iterator));
                }
            }

            private void __CommandInit(uint id, in NetworkConnection connection, ref NetworkDriverWrapper driver, ref UnsafeHashMap<NetworkPipeline, DataStreamWriter> writers)
            {
                if (initCommands.TryGetFirstValue(id, out var initCommand, out var iterator))
                {
                    var pipeline = manager.GetPipeline(id);

                    if (!writers.IsCreated)
                        writers = new UnsafeHashMap<NetworkPipeline, DataStreamWriter>(1, Allocator.Temp);

                    if (!writers.TryGetValue(pipeline, out var writer))
                        writer = default;

                    RPCMessage message;
                    message.type = (int)NetworkRPCType.Normal;
                    message.model = model;
                    message.maskIDs = default;
                    message.versions = versions;

                    int result;
                    StatusCode statusCode;

                    do
                    {
                        foreach (var initEvent in initEvents.GetValuesForKey(id))
                        {
                            if (initEvent.id == initCommand.id && initEvent.type == initCommand.type)
                            {
                                message.bytes = initCommand.bufferSegment.GetArray(buffer);
                                if (writer.IsCreated && writer.Capacity - writer.Length < message.size)
                                {
                                    result = driver.EndSend(writer);
                                    if (result < 0)
                                    {
                                        statusCode = (StatusCode)result;

                                        __LogError(statusCode);
                                    }

                                    writer = default;
                                }

                                if (!writer.IsCreated)
                                {
                                    statusCode = driver.BeginSend(pipeline, connection, out writer);
                                    if (StatusCode.Success != statusCode)
                                    {
                                        __LogError(statusCode);

                                        message.Dispose(initCommand.id, id);

                                        return;
                                    }
                                }

                                //Debug.Log($"Init {initCommand.id} To {id}");

                                message.Send(ref writer, initCommand.id, id);

                                break;
                            }
                        }
                    } while (initCommands.TryGetNextValue(out initCommand, ref iterator));

                    if (writer.IsCreated)
                        writers[pipeline] = writer;
                    /*result = driver.EndSend(writer);
                    if (result < 0)
                    {
                        statusCode = (StatusCode)result;

                        if (StatusCode.Success != statusCode)
                            __LogError(statusCode);
                    }*/
                    else
                        writers.Remove(pipeline);
                }
            }

            private void __CommandRPC(uint id, in NetworkConnection connection, ref NetworkDriverWrapper driver, ref UnsafeHashMap<NetworkPipeline, DataStreamWriter> writers)
            {
                NativeArray<RPCCommand> commands = default;
                int commandIndex = 0;
                foreach (var command in this.commands.GetValuesForKey(id))
                {
                    if (!commands.IsCreated)
                        commands = new NativeArray<RPCCommand>(this.commands.CountValuesForKey(id), Allocator.Temp, NativeArrayOptions.UninitializedMemory);

                    commands[commandIndex++] = command;
                }

                if (commands.IsCreated)
                {
                    if (!writers.IsCreated)
                        writers = new UnsafeHashMap<NetworkPipeline, DataStreamWriter>(1, Allocator.Temp);

                    commands.Sort();

                    int result;
                    StatusCode statusCode;
                    RPCMessage message;
                    NetworkPipeline pipeline = NetworkPipeline.Null;
                    DataStreamWriter writer = default;
                    foreach (var command in commands)
                    {
                        //Debug.LogError($"Send Rpc {command.bufferSegment.byteOffset} : {command.bufferSegment.length}");

                        message.type = command.type;
                        message.model = model;
                        message.maskIDs = command.maskIDs.GetArray<uint, NativeBuffer>(buffer);
                        message.bytes = command.bufferSegment.GetArray(buffer);
                        message.versions = versions;

                        if (message.IsVail(id))
                        {
                            if(writer.IsCreated)
                            {
                                if (command.pipeline != pipeline)
                                {
                                    writers[pipeline] = writer;

                                    pipeline = command.pipeline;

                                    if (!writers.TryGetValue(pipeline, out writer))
                                        writer = default;
                                }
                            }
                            else
                            {
                                pipeline = command.pipeline;

                                if (!writers.TryGetValue(pipeline, out writer))
                                    writer = default;
                            }

                            if (writer.IsCreated && writer.Capacity - writer.Length < message.size)
                            {
                                result = driver.EndSend(writer);
                                if (result < 0)
                                    __LogError((StatusCode)result);

                                writer = default;
                            }

                            if (!writer.IsCreated)
                            {
                                statusCode = driver.BeginSend(pipeline, connection, out writer);
                                if (StatusCode.Success != statusCode)
                                {
                                    writers.Remove(pipeline);

                                    __LogError(statusCode);
                                    
                                    message.Dispose(command.id, id);

                                    continue;
                                }
                            }

                            //UnityEngine.Debug.LogError($"Send Pos {command.bufferSegment.byteOffset}");

                            message.Send(ref writer, command.id, id);
                        }
                    }

                    commands.Dispose();

                    if (writer.IsCreated)
                        writers[pipeline] = writer;
                    /*{
                        result = driver.EndSend(writer);
                        if (result < 0)
                        {
                            statusCode = (StatusCode)result;

                            if (StatusCode.Success != statusCode)
                                __LogError(statusCode);
                        }
                    }*/
                }
            }

            private void __LogError(StatusCode statusCode)
            {
                UnityEngine.Debug.LogError($"Init Command: {(int)statusCode}");
            }
        }

        [BurstCompile]
        private struct Clear  :IJob
        {
            public NativeBuffer buffer;

            public NativeParallelMultiHashMap<uint, InitCommand> initCommands;

            public NativeParallelMultiHashMap<uint, InitEvent> initEvents;

            public void Execute()
            {
                buffer.Reset();
                buffer.length = 0;

                InitCommand initCommand;
                InitEvent initEvent;
                NativeParallelMultiHashMapIterator<uint> iterator;
                foreach (var pair in initCommands)
                {
                    if (initEvents.TryGetFirstValue(pair.Key, out initEvent, out iterator))
                    {
                        initCommand = pair.Value;
                        do
                        {
                            if (initEvent.id == initCommand.id && initEvent.type == initCommand.type)
                            {
                                initEvents.Remove(iterator);

                                break;
                            }
                        } while (initEvents.TryGetNextValue(out initEvent, ref iterator));
                    }
                }

                initCommands.Clear();
            }
        }

        public static readonly int SizeOfCommandHeader = UnsafeUtility.SizeOf<uint>() << 1;

        private NativeBuffer __buffer;

        private NativeArray<int> __rpcBufferCount;

        private NativeList<ActiveEvent> __activeEvents;

        private NativeList<uint> __rpcIDs;

        private NativeList<RPCCommand> __sourceRPCCommands;

        private NativeList<RegisterCommand> __registerCommands;
        private NativeList<UnregisterCommand> __unregisterCommands;

        private NativeList<NetworkDriverBuffer<uint>.Command> __bufferCommands;

        private NativeList<NetworkBuffer> __bufferPool;

        private NativeHashMap<uint, CommandIndex> __commandIndices;

        private NativeHashMap<uint, MoveCommand> __moveCommands;

        private NativeHashMap<uint, ReconnectCommand> __reconnectCommands;

        private NativeHashMap<uint, int> __types;

        private NativeHashMap<uint, int> __minCommandByteOffsets;

        private NativeHashMap<int, float> __nodeDistances;

        private NativeParallelMultiHashMap<uint, Version> __versions;

        private NativeParallelMultiHashMap<uint, InitCommand> __initCommands;

        private NativeParallelMultiHashMap<uint, InitEvent> __initEvents;

        private NativeParallelMultiHashMap<uint, uint> __idsToInit;

        private NativeParallelMultiHashMap<uint, RPCCommand> __destinationRPCCommands;

        private NativeParallelMultiHashMap<uint, NetworkBufferInstance> __rpcBufferInstanceInputs;

        private NativeParallelMultiHashMap<uint, NetworkBufferInstance> __rpcBufferInstanceOutputs;

        private NativeHashSet<uint> __idsToActive;

        private NativeHashSet<uint> __addIDs;

        private NativeHashSet<uint> __removeIDs;

        public NativeArray<ActiveEvent>.ReadOnly activeEventsReadOnly => __activeEvents.AsArray().AsReadOnly();

        public static int PayloadCapacity(in NetworkDriver.Concurrent driver, in NetworkPipeline pipeline)
        {
            int capacity = driver.PayloadCapacity(pipeline);
            //MSG TYPE
            capacity -= SizeOfCommandHeader;

            return capacity;
        }

        public NetworkRPCCommander(in AllocatorManager.AllocatorHandle allocator)
        {
            __buffer = new NativeBuffer(allocator, 1);

            __rpcBufferCount = new NativeArray<int>(1, allocator.ToAllocator, NativeArrayOptions.ClearMemory);

            __activeEvents = new NativeList<ActiveEvent>(allocator);

            __rpcIDs = new NativeList<uint>(allocator);

            __sourceRPCCommands = new NativeList<RPCCommand>(allocator);

            __registerCommands = new NativeList<RegisterCommand>(allocator);
            __unregisterCommands = new NativeList<UnregisterCommand>(allocator);

            __bufferCommands = new NativeList<NetworkDriverBuffer<uint>.Command>(allocator);

            __bufferPool = new NativeList<NetworkBuffer>(allocator);

            __commandIndices = new NativeHashMap<uint, CommandIndex>(1, allocator);

            __moveCommands = new NativeHashMap<uint, MoveCommand>(1, allocator);

            __reconnectCommands = new NativeHashMap<uint, ReconnectCommand>(1, allocator);

            __types = new NativeHashMap<uint, int>(1, allocator);

            __minCommandByteOffsets = new NativeHashMap<uint, int>(1, allocator);

            __nodeDistances = new NativeHashMap<int, float>(1, allocator);

            __versions = new NativeParallelMultiHashMap<uint, Version>(1, allocator);

            __initCommands = new NativeParallelMultiHashMap<uint, InitCommand>(1, allocator);

            __initEvents = new NativeParallelMultiHashMap<uint, InitEvent>(1, allocator);

            __idsToInit = new NativeParallelMultiHashMap<uint, uint>(1, allocator);

            __destinationRPCCommands = new NativeParallelMultiHashMap<uint, RPCCommand>(1, allocator);

            __rpcBufferInstanceInputs = new NativeParallelMultiHashMap<uint, NetworkBufferInstance>(1, allocator);
            __rpcBufferInstanceOutputs = new NativeParallelMultiHashMap<uint, NetworkBufferInstance>(1, allocator);

            __idsToActive = new NativeHashSet<uint>(1, allocator);

            __addIDs = new NativeHashSet<uint>(1, allocator);

            __removeIDs = new NativeHashSet<uint>(1, allocator);
        }

        public void Dispose()
        {
            __buffer.Dispose();
            __rpcBufferCount.Dispose();
            __activeEvents.Dispose();
            __sourceRPCCommands.Dispose();
            __rpcIDs.Dispose();
            __registerCommands.Dispose();
            __unregisterCommands.Dispose();
            __bufferCommands.Dispose();

            foreach (var buffer in __bufferPool)
                buffer.Dispose();

            __bufferPool.Dispose();

            __commandIndices.Dispose();
            __moveCommands.Dispose();
            __reconnectCommands.Dispose();
            __types.Dispose();
            __minCommandByteOffsets.Dispose();
            __nodeDistances.Dispose();
            __versions.Dispose();
            __initCommands.Dispose();
            __initEvents.Dispose();
            __idsToInit.Dispose();
            __destinationRPCCommands.Dispose();

            foreach (var rpcBuffer in __rpcBufferInstanceInputs)
                rpcBuffer.Value.buffer.Dispose();

            __rpcBufferInstanceInputs.Dispose();

            foreach (var rpcBuffer in __rpcBufferInstanceOutputs)
                rpcBuffer.Value.buffer.Dispose();

            __rpcBufferInstanceOutputs.Dispose();

            __idsToActive.Dispose();
            __addIDs.Dispose();
            __removeIDs.Dispose();
        }

        public bool BeginCommand(
            uint id, 
            int capacity, 
            in NetworkPipeline pipeline, 
            out DataStreamWriter stream)
        {
            //int capacity = driver.PayloadCapacity(pipeline);
            //MSG TYPE
            //capacity -= SizeOfCommandHeader;

            if (capacity > 0)
            {
                int position = __buffer.position;

                var writer = __buffer.writer;

                writer.Write(capacity);
                writer.Write(id);
                writer.Write(pipeline);

                var array = writer.WriteArray<byte>(capacity, NativeArrayOptions.UninitializedMemory);

                stream = DataStreamUtility.CreateWriter(array, (IntPtr)position);

                return true;
            }

            stream = default;

            return false;
        }
        
        /*public bool BeginCommand(
            uint id, 
            in NetworkPipeline pipeline, 
            in NetworkDriver driver, 
            out DataStreamWriter stream)
        {
            return BeginCommand(id, PayloadCapacity(driver, pipeline), pipeline, out stream);
        }*/

        public int EndCommandInit(InitType type, uint targetID, DataStreamWriter writer)
        {
            if (!__EndCommand(writer, out int position, out uint id, out var pipeline))
                return (int)StatusCode.NetworkPacketOverflow;

            InitCommand command;
            command.type = type;
            command.id = targetID;
            command.pipeline = pipeline;
            command.bufferSegment.byteOffset = __buffer.position;
            command.bufferSegment.length = writer.Length;

            __initCommands.Add(id, command);

            __buffer.position = position;

            return command.bufferSegment.length + SizeOfCommandHeader;
        }

        public int EndCommandRPC(
            int type, 
            DataStreamWriter writer, 
            in NativeArray<uint> additionalIDs = default, 
            in NativeArray<uint> maskIDs = default)
        {
            if (!__EndCommand(writer, out int position, out uint id, out var pipeline))
                return (int)StatusCode.NetworkPacketOverflow;

            //UnityEngine.Debug.LogError($"Buffer Pos {__buffer.position}");

            RPCCommand command;
            command.type = type;
            command.id = id;
            command.pipeline = pipeline;
            command.bufferSegment.byteOffset = __buffer.position;
            command.bufferSegment.length = writer.Length;

            __buffer.position = position;
            if (additionalIDs.IsCreated)
            {
                command.additionalIDs.byteOffset = position;
                command.additionalIDs.length = additionalIDs.Length;

                var stream = __buffer.writer;
                foreach (uint additionalID in additionalIDs)
                    stream.Write(additionalID);
            }
            else
                command.additionalIDs = default;

            if (maskIDs.IsCreated)
            {
                command.maskIDs.byteOffset = __buffer.position;
                command.maskIDs.length = maskIDs.Length;

                var stream = __buffer.writer;
                foreach (uint maskID in maskIDs)
                    stream.Write(maskID);
            }
            else
                command.maskIDs = default;

            __sourceRPCCommands.Add(command);
            
            //Debug.LogError($"Rpc {command.bufferSegment.byteOffset}");

            return command.bufferSegment.length + SizeOfCommandHeader;
        }

        public int EndCommandRegister(
            int type, 
            int node, 
            int layer, 
            int layerMask, 
            float visibilityDistance, 
            in NetworkConnection connection,
            DataStreamWriter writer)
        {
            if (!__EndCommand(writer, out int position, out uint id, out var pipeline))
                return (int)StatusCode.NetworkPacketOverflow;

            CommandIndex commandIndex;
            commandIndex.type = CommandType.Register;
            commandIndex.value = __registerCommands.Length;
            __commandIndices[id] = commandIndex;

            RegisterCommand command;
            command.type = type;
            command.node = node;
            command.layer = layer;
            command.layerMask = layerMask;
            command.visibilityDistance = visibilityDistance;
            command.connection = connection;
            command.pipeline = pipeline;

            command.bufferSegment.byteOffset = __buffer.position;
            command.bufferSegment.length = writer.Length;

            __buffer.position = position;

            __registerCommands.Add(command);

            __minCommandByteOffsets[id] = command.bufferSegment.byteOffset;

            __reconnectCommands.Remove(id);

            __moveCommands.Remove(id);

            __initCommands.Remove(id);

            return command.bufferSegment.length + SizeOfCommandHeader;
        }

        public int EndCommandUnregister(DataStreamWriter writer)
        {
            if (!__EndCommand(writer, out int position, out uint id, out var pipeline))
                return (int)StatusCode.NetworkPacketOverflow;

            CommandIndex commandIndex;
            commandIndex.type = CommandType.Unregister;
            commandIndex.value = __unregisterCommands.Length;
            __commandIndices[id] = commandIndex;

            UnregisterCommand command;
            command.pipeline = pipeline;

            command.bufferSegment.byteOffset = __buffer.position;
            command.bufferSegment.length = writer.Length;

            __buffer.position = position;

            __unregisterCommands.Add(command);

            __minCommandByteOffsets[id] = int.MaxValue;

            __reconnectCommands.Remove(id);

            __moveCommands.Remove(id);

            __initCommands.Remove(id);

            return command.bufferSegment.length + SizeOfCommandHeader;
        }

        public int EndCommandMove(
            int registerMessageLength, 
            int node, 
            int layerMask, 
            float visibilityDistance, 
            DataStreamWriter writer)
        {
            if (!__EndCommand(writer, out int position, out uint id, out var pipeline))
                return (int)StatusCode.NetworkPacketOverflow;

            MoveCommand moveCommand;
            moveCommand.node = node;
            moveCommand.layerMask = layerMask;
            moveCommand.visibilityDistance = visibilityDistance;
            moveCommand.pipeline = pipeline;

            moveCommand.registerBufferSegment.byteOffset = __buffer.position;
            moveCommand.registerBufferSegment.length = registerMessageLength;

            moveCommand.unregisterBufferSegment.byteOffset = moveCommand.registerBufferSegment.byteOffset + moveCommand.registerBufferSegment.length;
            moveCommand.unregisterBufferSegment.length = writer.Length - math.max(registerMessageLength, 0);

            __buffer.position = position;

            UnityEngine.Assertions.Assert.IsFalse(moveCommand.unregisterBufferSegment.length < 0);

            __moveCommands[id] = moveCommand;

            return moveCommand.registerBufferSegment.length + moveCommand.unregisterBufferSegment.length + SizeOfCommandHeader;
        }

        public int EndCommandReconnect(
            int registerMessageLength,
            int type,
            in NetworkConnection connection,
            DataStreamWriter writer)
        {
            if (!__EndCommand(writer, out int position, out uint id, out var pipeline))
                return (int)StatusCode.NetworkPacketOverflow;

            ReconnectCommand reconnectCommand;
            reconnectCommand.type = type;
            reconnectCommand.connection = connection;
            reconnectCommand.pipeline = pipeline;

            reconnectCommand.registerBufferSegment.byteOffset = __buffer.position;
            reconnectCommand.registerBufferSegment.length = registerMessageLength;

            reconnectCommand.unregisterBufferSegment.byteOffset = reconnectCommand.registerBufferSegment.byteOffset + reconnectCommand.registerBufferSegment.length;
            reconnectCommand.unregisterBufferSegment.length = writer.Length - math.max(registerMessageLength, 0);
            
            __buffer.position = position;

            UnityEngine.Assertions.Assert.IsFalse(reconnectCommand.unregisterBufferSegment.length < 0);

            __reconnectCommands[id] = reconnectCommand;

            __minCommandByteOffsets[id] = reconnectCommand.unregisterBufferSegment.byteOffset;

            __initCommands.Remove(id);

            return reconnectCommand.registerBufferSegment.length + reconnectCommand.unregisterBufferSegment.length + SizeOfCommandHeader;
        }

        public int EndCommandConnect(
            int type,
            in NetworkConnection connection,
            DataStreamWriter writer)
        {
            return EndCommandReconnect(writer.Length, type, connection, writer);
        }

        public int EndCommandDisconnect(int type, DataStreamWriter writer)
        {
            return EndCommandReconnect(-1, type, default, writer);
        }

        public void AbortCommand(DataStreamWriter writer)
        {
            int destinationPosition = __buffer.position, 
                sourcePosition = (int)writer.GetSendHandleData();

            __buffer.position = sourcePosition;

            var reader = __buffer.reader;
            int capacity = reader.Read<int>();
            if (sourcePosition + capacity >= destinationPosition)
                __buffer.position = sourcePosition;
        }

        public void GetInitIDs(ref NativeList<uint> ids)
        {
            uint id;
            var enumerator = __initEvents.GetEnumerator();
            while (enumerator.MoveNext())
            {
                id = enumerator.Current.Key;
                if (ids.IndexOf(id) == -1)
                    ids.Add(id);
            }
        }

        public NativeParallelMultiHashMap<uint, InitEvent>.Enumerator GetInitEvents(uint id)
        {
            return __initEvents.GetValuesForKey(id);
        }

        public JobHandle ScheduleCommand(
            ref NetworkRPCManager<int> manager,
            in NativeGraphEx<int> graph,
            in NativeHashMap<NetworkConnection, uint> ids,
            in NetworkDriver.Concurrent driver,
            in JobHandle inputDeps)
        {
            Command command;
            command.ids = ids;
            command.graph = graph;
            command.buffer = __buffer;
            command.driver = driver;
            command.model = StreamCompressionModel.Default;
            command.rpcBufferCount = __rpcBufferCount;
            command.rpcIDs = __rpcIDs;
            command.activeEvents = __activeEvents;
            command.registerCommands = __registerCommands;
            command.unregisterCommands = __unregisterCommands;
            command.bufferCommands = __bufferCommands;
            command.bufferPool = __bufferPool;
            command.commandIndices = __commandIndices;
            command.moveCommands = __moveCommands;
            command.reconnectCommands = __reconnectCommands;
            command.types = __types;
            command.nodeDistances = __nodeDistances;
            command.versions = __versions;
            command.bufferInstanceInputs = __rpcBufferInstanceInputs;
            command.bufferInstanceOutputs = __rpcBufferInstanceOutputs;
            command.initEvents = __initEvents;
            command.idsToInit = __idsToInit;
            command.idsToActive = __idsToActive;
            command.addIDs = __addIDs;
            command.removeIDs = __removeIDs;
            command.manager = manager;

            return command.ScheduleByRef(inputDeps);
        }

        public JobHandle ScheduleCommandRPC(
            int innerloopBatchCount,
            ref NetworkDriver.Concurrent driver,
            in NetworkRPCManager<int>.ReadOnly manager,
            in JobHandle inputDeps)
        {
            var model = StreamCompressionModel.Default;

            /*var ids = __initCommands.GetKeyArray(Allocator.TempJob);

            CommandInit commandInit;
            commandInit.model = model;
            commandInit.manager = managerReadOnly;
            commandInit.driver = driver;
            commandInit.buffer = __buffer;
            commandInit.ids = ids;
            commandInit.initCommands = __initCommands;
            commandInit.initEvents = __initEvents;
            var jobHandle = commandInit.Schedule(ids.ConvertToUniqueArray(), innerloopBatchCount, inputDeps);*/

            CollectRPC collectRPC;
            collectRPC.driver = driver;
            collectRPC.initCommands = __initCommands;
            collectRPC.buffer = __buffer;
            collectRPC.manager = manager;
            collectRPC.minCommandByteOffsets = __minCommandByteOffsets;
            collectRPC.bufferInstanceResults = __rpcBufferInstanceOutputs;
            collectRPC.destinations = __destinationRPCCommands;
            collectRPC.sources = __sourceRPCCommands;
            collectRPC.rpcIDs = __rpcIDs;
            var jobHandle = collectRPC.ScheduleByRef(inputDeps);

            /*CommandRPC commandRPC;
            commandRPC.model = model;
            commandRPC.manager = managerReadOnly;
            commandRPC.driver = driver;
            commandRPC.buffer = __buffer;
            commandRPC.rpcIDs = __rpcIDs.AsDeferredJobArrayEx();
            commandRPC.commands = __destinationRPCCommands;*/
            CommandParallel command;
            command.model = model;
            command.manager = manager;
            command.driver = driver;
            command.buffer = __buffer;
            command.rpcBufferPool = __bufferPool.AsDeferredJobArray();
            command.rpcIDs = __rpcIDs.AsDeferredJobArray();
            command.versions = __versions;
            command.initCommands = __initCommands;
            command.initEvents = __initEvents;
            command.commands = __destinationRPCCommands;
            command.rpcBufferInstanceInputs = __rpcBufferInstanceInputs;
            command.rpcBufferInstanceOutputs = __rpcBufferInstanceOutputs.AsParallelWriter();
            command.rpcBufferPoolCount = __rpcBufferCount;

            jobHandle = command.ScheduleByRef(__rpcIDs, innerloopBatchCount, jobHandle);

            Clear clear;
            clear.buffer = __buffer;
            clear.initCommands = __initCommands;
            clear.initEvents = __initEvents;

            return clear.ScheduleByRef(jobHandle);
        }

        private bool __EndCommand(DataStreamWriter writer, out int position, out uint id, out NetworkPipeline pipeline)
        {
            position = __buffer.position;
            int originPosition = (int)writer.GetSendHandleData();
            __buffer.position = originPosition;

            var reader = __buffer.reader;
            int capacity = reader.Read<int>();
            id = reader.Read<uint>();
            pipeline = reader.Read<NetworkPipeline>();

            if (writer.HasFailedWrites)
            {
                if (originPosition + capacity >= position)
                {
                    position = originPosition;

                    __buffer.position = position;
                }

                return false;
            }

            if (originPosition + capacity >= position)
                position = originPosition + writer.Length;

            return true;
        }
    }

    public struct NetworkRPCController : IComponentData
    {
        private UnsafeList<LookupJobManager> __lookupJobManager;

        public bool isCreated => __lookupJobManager.IsCreated;

        public NativeGraphEx<int> graph
        {
            get;
        }

        public NetworkRPCManager<int> manager
        {
            get;
        }

        public NetworkRPCCommander commander
        {
            get;
        }

        public ref LookupJobManager lookupJobManager => ref __lookupJobManager.ElementAt(0);

        public static EntityQuery GetEntityQuery(ref SystemState state)
        {
            using (var builder = new EntityQueryBuilder(Allocator.Temp))
                return builder.WithAll<NetworkRPCController>()
                    .WithOptions(EntityQueryOptions.IncludeSystems)
                    .Build(ref state);
        }

        public NetworkRPCController(AllocatorManager.AllocatorHandle allocator)
        {
            __lookupJobManager = new UnsafeList<LookupJobManager>(1, allocator, NativeArrayOptions.UninitializedMemory);
            __lookupJobManager.Resize(1, NativeArrayOptions.ClearMemory);

            graph = new NativeGraphEx<int>(allocator);
            manager = new NetworkRPCManager<int>(allocator);
            commander = new NetworkRPCCommander(allocator);
        }

        public void Dispose()
        {
            lookupJobManager.CompleteReadWriteDependency();

            __lookupJobManager.Dispose();

            graph.Dispose();
            manager.Dispose();
            commander.Dispose();
        }

        public JobHandle Update(
            in NativeHashMap<NetworkConnection, uint> ids,
            in NetworkDriver.Concurrent driver,
            in JobHandle inputDeps)
        {
            ref var lookupJobManager = ref this.lookupJobManager;

            var jobHandle = JobHandle.CombineDependencies(lookupJobManager.readWriteJobHandle, inputDeps);

            var manager = this.manager;

            jobHandle = commander.ScheduleCommand(ref manager, graph, ids, driver, jobHandle);

            lookupJobManager.readWriteJobHandle = jobHandle;

            return jobHandle;
        }
    }

    [AutoCreateIn("Server"), BurstCompile]
    public partial struct NetworkRPCFactorySystem : ISystem
    {
        public NetworkRPCController controller
        {
            get;

            private set;
        }

        private EntityQuery __managerGroup;

        [BurstCompile]
        public void OnCreate(ref SystemState state)
        {
            __managerGroup = NetworkServerManager.GetEntityQuery(ref state);

            state.EntityManager.AddComponentData(state.SystemHandle, controller = new NetworkRPCController(Allocator.Persistent));
        }

        [BurstCompile]
        public void OnDestroy(ref SystemState state)
        {
            controller.Dispose();
        }

        [BurstCompile]
        public void OnUpdate(ref SystemState state)
        {
            if (!__managerGroup.HasSingleton<NetworkServerManager>())
                return;

            var manager = __managerGroup.GetSingleton<NetworkServerManager>();

            var server = manager.server;

            ref var lookupJobManager = ref manager.lookupJobManager;
            var jobHandle = controller.Update(
                server.ids,
                server.driverConcurrent, 
                JobHandle.CombineDependencies(lookupJobManager.readWriteJobHandle, state.Dependency));

            lookupJobManager.readWriteJobHandle = jobHandle;

            state.Dependency = jobHandle;
        }
    }

    [AutoCreateIn("Server"), BurstCompile, UpdateBefore(typeof(NetworkServerSystem)), UpdateAfter(typeof(NetworkRPCFactorySystem))]
    public partial struct NetworkRPCSystem : ISystem
    {
        public static readonly int InnerloopBatchCount = 4;

        private EntityQuery __managerGroup;

        private EntityQuery __controllerGroup;

        [BurstCompile]
        public void OnCreate(ref SystemState state)
        {
            __managerGroup = NetworkServerManager.GetEntityQuery(ref state);
            __controllerGroup = NetworkRPCController.GetEntityQuery(ref state);
        }

        [BurstCompile]
        public void OnDestroy(ref SystemState state)
        {

        }

        [BurstCompile]
        public void OnUpdate(ref SystemState state)
        {
            if (!__managerGroup.HasSingleton<NetworkServerManager>())
                return;

            var manager = __managerGroup.GetSingleton<NetworkServerManager>();
            var controller = __controllerGroup.GetSingleton<NetworkRPCController>();

            ref var controllerJobManager = ref controller.lookupJobManager;

            var jobHandle = JobHandle.CombineDependencies(controllerJobManager.readWriteJobHandle, state.Dependency);

            var driver = manager.server.driverConcurrent;
            jobHandle = controller.commander.ScheduleCommandRPC(InnerloopBatchCount, ref driver, controller.manager.AsReadOnly(), jobHandle);

            controllerJobManager.readWriteJobHandle = jobHandle;

            manager.lookupJobManager.readWriteJobHandle = jobHandle;

            state.Dependency = jobHandle;
        }
    }
}