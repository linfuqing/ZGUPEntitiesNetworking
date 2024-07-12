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

    public interface INetworkRPCDriver
    {
        NetworkConnection.State GetConnectionState(in NetworkConnection connection);

        StatusCode BeginSend(in NetworkPipeline pipe, in NetworkConnection id, out DataStreamWriter writer, int requiredPayloadSize = 0);

        int EndSend(DataStreamWriter writer);

        void AbortSend(DataStreamWriter writer);
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
        /// id��Ӧ�ɿ���������
        /// </summary>
        private NativeParallelMultiHashMap<uint, T> __idNodes;

        /// <summary>
        /// �ɿ����������id
        /// </summary>
        private NativeParallelMultiHashMap<T, uint> __nodeIDs;

        /// <summary>
        /// ��������ڵ�Identity
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
        /// id��Ӧ�ɿ���������
        /// </summary>
        public NativeParallelMultiHashMap<uint, T>.Enumerator GetIDNodes(uint id)
        {
            return __idNodes.GetValuesForKey(id);
        }

        /// <summary>
        /// �ɿ����������id
        /// </summary>
        public NativeParallelMultiHashMap<T, uint>.Enumerator GetNodeIDs(in T value)
        {
            return __nodeIDs.GetValuesForKey(value);
        }

        /// <summary>
        /// ��������ڵ�Identity
        /// </summary>
        public NativeParallelMultiHashMap<T, NodeIdentity>.Enumerator GetNodeIdentities(in T value)
        {
            return __nodeIdentities.GetValuesForKey(value);
        }

        public bool IsActive<TDriver>(uint id, in TDriver driver) where TDriver : INetworkRPCDriver
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
            where TDriver : INetworkRPCDriver
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
            where TDriver : INetworkRPCDriver
            where TRegisterMessage : INetworkRPCMessage
            where TUnregisterMessage : INetworkRPCMessage
        {
            if (!__identities.TryGetValue(id, out var identity))
                return false;

            if (identity.connection == connection)
                return true;

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
            where TDriver : INetworkRPCDriver
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

            bool isSend = identity.connection.IsCreated/*NetworkConnection.State.Connected == driver.GetConnectionState(identity.connection)*/ && message.IsVail(id);
            int messageSize = message.size, result;
            StatusCode statusCode;
            NodeIdentity nodeIdentity;
            T targetNode;
            NativeParallelMultiHashMapIterator<T> iterator;
            DataStreamWriter writer = default;
            foreach (var nodeDistance in nodeDistances)
            {
                targetNode = nodeDistance.Key;

                if (isSend)
                {
                    //�Լ��ܿ���������
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

                                        message.Dispose(nodeIdentity.value, id);
                                        while (__nodeIdentities.TryGetNextValue(out nodeIdentity, ref iterator))
                                        {
                                            if ((layerMask & (1 << nodeIdentity.layer)) != 0)
                                                message.Dispose(nodeIdentity.value, id);
                                        }

                                        break;
                                    }
                                }

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

                    if (StatusCode.Success != statusCode)
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
            where TDriver : INetworkRPCDriver
            where TMessage : INetworkRPCMessage
        {
            if (!__GetIdentity(id, out int layer, out var identity, out var nodeIterator))
                return false;

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

                                            message.Dispose(nodeIdentity.value, id);
                                            while (__nodeIdentities.TryGetNextValue(out nodeIdentity, ref nodeIterator))
                                            {
                                                if ((identity.layerMask & (1 << nodeIdentity.layer)) != 0)
                                                    message.Dispose(nodeIdentity.value, id);
                                            }

                                            break;
                                        }
                                    }

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
            where TDriver : INetworkRPCDriver
            where TRegisterMessage : INetworkRPCMessage
            where TUnregisterMessage : INetworkRPCMessage
        {
            if (!__GetIdentity(id, out int layer, out var identity, out var nodeIterator))
                return false;
            
            //Debug.Log($"Move {id} From {identity.node} To {node}");

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

            bool isConnected = identity.connection.IsCreated, //NetworkConnection.State.Connected == driver.GetConnectionState(identity.connection),
                isUnregistered = isConnected && unregisterMessage.IsVail(id);
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
                        if (isUnregistered && __nodeIdentities.TryGetFirstValue(sourceNode, out nodeIdentity, out nodeIterator))
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
                                            //isConnected = false;

                                            isUnregistered = false;

                                            writer = default;

                                            __LogUnregisterError(statusCode);

                                            unregisterMessage.Dispose(nodeIdentity.value, id);
                                            while (__nodeIdentities.TryGetNextValue(out nodeIdentity, ref nodeIterator))
                                            {
                                                if ((identity.layerMask & (1 << nodeIdentity.layer)) != 0)
                                                    unregisterMessage.Dispose(nodeIdentity.value, id);
                                            }
                                            
                                            break;
                                        }
                                    }

                                    unregisterMessage.Send(ref writer, nodeIdentity.value, id);
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

            bool isRegistered = isConnected && registerMessage.IsVail(id), isContains;
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

                    if (isRegistered)
                    {   
                        if (__nodeIdentities.TryGetFirstValue(destinationNode, out nodeIdentity, out nodeIterator))
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
                                        statusCode = driver.BeginSend(identity.pipeline, identity.connection, out writer);
                                        if (StatusCode.Success != statusCode)
                                        {
                                            //isConnected = false;

                                            isRegistered = false;

                                            writer = default;

                                            __LogRegisterError(statusCode);

                                            registerMessage.Dispose(nodeIdentity.value, id);
                                            while (__nodeIdentities.TryGetNextValue(out nodeIdentity, ref nodeIterator))
                                            {
                                                if ((layerMask & (1 << nodeIdentity.layer)) != 0)
                                                    registerMessage.Dispose(nodeIdentity.value, id);
                                            }
                                            break;
                                        }
                                    }

                                    registerMessage.Send(ref writer, nodeIdentity.value, id);
                                }
                            } while (__nodeIdentities.TryGetNextValue(out nodeIdentity, ref nodeIterator));
                        }
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
                    statusCode = driver.BeginSend(targetIdentity.pipeline, targetIdentity.connection, out writer);
                    if (StatusCode.Success == statusCode)
                    {
                        unregisterMessage.Send(ref writer, id, removeID);

                        result = driver.EndSend(writer);
                        if (result < 0)
                            statusCode = (StatusCode)result;
                    }
                    else
                        unregisterMessage.Dispose(id, removeID);

                    if (StatusCode.Success != statusCode)
                        Debug.LogError($"[Move]Unregister {id} for {removeID}: {statusCode}");
                }
            }

            foreach (uint addID in addIDs)
            {
                if (registerMessage.IsVail(addID) && 
                    __identities.TryGetValue(addID, out targetIdentity) &&
                    targetIdentity.connection.IsCreated)
                {
                    statusCode = driver.BeginSend(targetIdentity.pipeline, targetIdentity.connection, out writer);
                    if (StatusCode.Success == statusCode)
                    {
                        registerMessage.Send(ref writer, id, addID);

                        result = driver.EndSend(writer);
                        if (result < 0)
                            statusCode = (StatusCode)result;
                    }
                    else
                        registerMessage.Dispose(id, addID);

                    if (StatusCode.Success != statusCode)
                        Debug.LogError($"[Move]Register {id} for {addID}: {statusCode}");
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
            where TDriver : INetworkRPCDriver
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

        private struct BufferSegment
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

        private struct RPCCommand : IComparable<RPCCommand>
        {
            public int type;

            public uint id;

            public NetworkPipeline pipeline;

            public BufferSegment bufferSegment;
            public BufferSegment additionalIDs;
            public BufferSegment maskIDs;

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

            public BufferSegment bufferSegment;
        }

        private struct UnregisterCommand
        {
            public NetworkPipeline pipeline;

            public BufferSegment bufferSegment;
        }

        private struct ReconnectCommand
        {
            public int type;
            public NetworkConnection connection;

            public NetworkPipeline pipeline;

            public BufferSegment registerBufferSegment;
            public BufferSegment unregisterBufferSegment;
        }

        private struct MoveCommand
        {
            public int node;
            public int layerMask;
            public float visibilityDistance;

            public NetworkPipeline pipeline;

            public BufferSegment registerBufferSegment;
            public BufferSegment unregisterBufferSegment;
        }

        private struct InitCommand
        {
            public InitType type;
            public uint id;

            public NetworkPipeline pipeline;

            public BufferSegment bufferSegment;
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
                Debug.Log($"[RPC]Register {sourceID} To {destinationID}");

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
                Debug.Log($"[RPC]Register {sourceID} To {destinationID}.");

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
                //Debug.Log($"Unregister {sourceID} To {destinationID}");

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
                
                Debug.Log($"[RPC]Unregister {sourceID} To {destinationID}.");
                
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
                //Debug.Log($"Unregister {sourceID} To {destinationID}");

                Version version;
                version.id = sourceID;

                version.value = Version.Get(
                    versions,
                    sourceID,
                    destinationID,
                    out var iterator, 
                    out bool isActive);
                
                Debug.Log($"[RPC]Unregister {sourceID} To {destinationID}.");
                
                if(!isActive)
                    Debug.LogError($"[RPC]Unregister {sourceID} To {destinationID} with an inactive version.");
                
                UnityEngine.Assertions.Assert.AreNotEqual(0, version.value);
                //UnityEngine.Assertions.Assert.IsTrue(isActive);

                version.isActive = false;

                versions.SetValue(version, iterator);
#endif
            }
        }

        private struct RPCBufferCommand
        {
#if DEBUG
            public bool isActive;
#endif
            public uint id;
            public NetworkPipeline pipeline;
            public BufferSegment bufferSegment;
        }

        private struct RPCBuffer
        {
            public UnsafeList<BufferSegment> segments;
            public UnsafeBuffer value;

            public RPCBuffer(int initialCapacity, AllocatorManager.AllocatorHandle allocator)
            {
                segments = new UnsafeList<BufferSegment>(1, allocator, NativeArrayOptions.UninitializedMemory);
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
                ref DataStreamWriter writer) where T : INetworkRPCDriver
            {
                int numSegments = segments.Length, result;
                StatusCode statusCode;
                bool isSend;
                for(int i = 0; i < numSegments; ++i)
                {
                    if (writer.IsCreated)
                        isSend = false;
                    else
                    {
                        statusCode = driver.BeginSend(pipeline, connection, out writer);
                        if (StatusCode.Success != statusCode)
                        {
                            __LogError(statusCode);

                            writer = default;

                            break;
                        }

                        isSend = true;
                    }

                    ref readonly var setment = ref segments.ElementAt(i);
                    if(setment.length > writer.Capacity - writer.Length)
                    {
                        if (isSend)
                        {
                            driver.AbortSend(writer);

                            writer = default;

                            __LogError(StatusCode.NetworkPacketOverflow);

                            break;
                        }

                        result = driver.EndSend(writer);
                        if (result < 0)
                            __LogError((StatusCode)result);

                        writer = default;

                        --i;
                    }
                    else
                        writer.WriteBytes(setment.GetArray(value));
                }
            }

            private void __LogError(StatusCode statusCode)
            {
                UnityEngine.Debug.LogError($"RPC: {(int)statusCode}");
            }
        }

        private struct RPCBufferInstance
        {
            public NetworkPipeline pipeline;
            public RPCBuffer buffer;
        }

        [BurstCompile]
        private struct Command : IJob
        {
            private struct Driver : INetworkRPCDriver
            {
                public NetworkDriver.Concurrent instance;
                public NativeList<RPCBuffer> bufferPool;
                public NativeList<RPCBufferCommand> commands;
                public NativeParallelMultiHashMap<uint, RPCBufferInstance> bufferInstances;
                public NativeHashMap<NetworkConnection, uint> ids;

                public bool TryGetBuffer(
                    uint id,
                    in NetworkPipeline pipeline,
                    out RPCBufferInstance bufferInstance,
                    out NativeParallelMultiHashMapIterator<uint> iterator)
                {
                    if (bufferInstances.TryGetFirstValue(id, out bufferInstance, out iterator))
                    {
                        do
                        {
                            if (bufferInstance.pipeline == pipeline)
                            {
                                return true;
                            }

                        } while (bufferInstances.TryGetNextValue(out bufferInstance, ref iterator));
                    }

                    return false;
                }

                public NetworkConnection.State GetConnectionState(in NetworkConnection connection)
                {
                    return instance.GetConnectionState(connection);
                }

                public StatusCode BeginSend(
                    in NetworkPipeline pipeline,
                    in NetworkConnection connection,
                    out DataStreamWriter writer,
                    int requiredPayloadSize = 0)
                {
                    if (!ids.TryGetValue(connection, out uint id))
                    {
                        writer = default;

                        return StatusCode.NetworkIdMismatch;
                    }

                    RPCBufferCommand command;
                    command.bufferSegment.length = instance.PayloadCapacity(pipeline);
                    if(requiredPayloadSize > 0)
                    {
                        if (requiredPayloadSize > command.bufferSegment.length)
                        {
                            writer = default;

                            return StatusCode.NetworkPacketOverflow;
                        }

                        command.bufferSegment.length = requiredPayloadSize;
                    }

                    if (TryGetBuffer(id, pipeline, out var bufferInstance, out var iterator))
                        bufferInstances.Remove(iterator);
                    else
                    {
                        bufferInstance.pipeline = pipeline;

                        int bufferPoolLength = bufferPool.Length;
                        if (bufferPoolLength > 0)
                        {
                            int bufferIndex = bufferPoolLength - 1;

                            bufferInstance.buffer = bufferPool[bufferIndex];
                            bufferInstance.buffer.Clear();

                            bufferPool.ResizeUninitialized(bufferIndex);
                        }
                        else
                            bufferInstance.buffer = new RPCBuffer(command.bufferSegment.length, Allocator.Persistent);
                    }

                    command.bufferSegment.byteOffset = bufferInstance.buffer.value.length;
                    command.pipeline = pipeline;
                    command.id = id;

#if DEBUG
                    command.isActive = true;
#endif

                    commands.Add(command);

                    writer = DataStreamUtility.CreateWriter(
                        bufferInstance.buffer.value.writer.WriteBlock(command.bufferSegment.length, false).AsArray<byte>(),
                        (IntPtr)commands.Length);

                    bufferInstances.Add(id, bufferInstance);

                    return StatusCode.Success;
                }

                public int EndSend(DataStreamWriter writer)
                {
                    int commandIndex = (int)writer.GetSendHandleData() - 1;
                    var command = commands[commandIndex];

#if DEBUG
                    UnityEngine.Assertions.Assert.IsTrue(command.isActive);

                    command.isActive = false;
                    commands[commandIndex] = command;

                    writer.SetSendHandleData(IntPtr.Zero);
#endif

                    if (!TryGetBuffer(command.id, command.pipeline, out var bufferInstance, out var iterator))
                        return (int)StatusCode.NetworkSendHandleInvalid;

                    int length = writer.Length;

#if DEBUG
                    if (length > NetworkParameterConstants.MTU)
                        Debug.LogWarning("Long Stream.");
#endif

                    UnityEngine.Assertions.Assert.AreEqual(bufferInstance.buffer.value.length, command.bufferSegment.byteOffset + command.bufferSegment.length);

                    bufferInstance.buffer.value.length = command.bufferSegment.byteOffset + length;

                    command.bufferSegment.length = length;

                    bufferInstance.buffer.segments.Add(command.bufferSegment);

                    bufferInstances.SetValue(bufferInstance, iterator);

                    return length;
                }

                public void AbortSend(DataStreamWriter writer)
                {
                    int commandIndex = (int)writer.GetSendHandleData() - 1;
                    var command = commands[commandIndex];

#if DEBUG
                    command.isActive = false;
                    commands[commandIndex] = command;

                    writer.SetSendHandleData(IntPtr.Zero);
#endif

                    if (!TryGetBuffer(command.id, command.pipeline, out var bufferInstance, out var iterator))
                        return;

                    UnityEngine.Assertions.Assert.AreEqual(bufferInstance.buffer.value.length, command.bufferSegment.byteOffset + command.bufferSegment.length);

                    bufferInstance.buffer.value.length = command.bufferSegment.byteOffset;

                    bufferInstances.SetValue(bufferInstance, iterator);
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

            public NativeList<RPCBufferCommand> rpcBufferCommands;
            public NativeList<RPCBuffer> rpcBufferPool;

            public NativeHashMap<uint, CommandIndex> commandIndices;
            public NativeHashMap<uint, MoveCommand> moveCommands;
            public NativeHashMap<uint, ReconnectCommand> reconnectCommands;

            public NativeHashMap<uint, int> types;

            public NativeHashMap<int, float> nodeDistances;

            public NativeParallelMultiHashMap<uint, Version> versions;

            public NativeParallelMultiHashMap<uint, RPCBufferInstance> rpcBufferInstanceInputs;

            public NativeParallelMultiHashMap<uint, RPCBufferInstance> rpcBufferInstanceOutputs;

            public NativeParallelMultiHashMap<uint, InitEvent> initEvents;

            public NativeParallelMultiHashMap<uint, uint> idsToInit;

            public NativeHashSet<uint> idsToActive;

            public NativeHashSet<uint> addIDs;

            public NativeHashSet<uint> removeIDs;

            public NetworkRPCManager<int> manager;

            public void BeginRPC()
            {
                rpcBufferCommands.Clear();

                UnityEngine.Assertions.Assert.IsFalse(rpcBufferCount[0] > rpcBufferPool.Length);

                rpcBufferPool.ResizeUninitialized(rpcBufferCount[0]);

                foreach (var rpcBufferInstance in rpcBufferInstanceInputs)
                    rpcBufferPool.Add(rpcBufferInstance.Value.buffer);

                rpcBufferInstanceInputs.Clear();

                foreach (var rpcBufferInstance in rpcBufferInstanceOutputs)
                    rpcBufferInstanceInputs.Add(rpcBufferInstance.Key, rpcBufferInstance.Value);

                rpcBufferInstanceOutputs.Clear();
            }

            public void EndRPC()
            {
                rpcIDs.Clear();
                using (var ids = rpcBufferInstanceInputs.GetKeyArray(Allocator.Temp))
                {
                    int count = ids.ConvertToUniqueArray();
                    rpcIDs.AddRange(ids.GetSubArray(0, count));
                }

                rpcBufferCount[0] = rpcBufferPool.Length;

#if DEBUG
                foreach (var bufferCommand in rpcBufferCommands)
                    UnityEngine.Assertions.Assert.IsFalse(bufferCommand.isActive);
#endif
            }

            public void Execute()
            {
                BeginRPC();

                activeEvents.Clear();
                //initEvents.Clear();

                Driver driver;
                driver.instance = this.driver;
                driver.bufferPool = rpcBufferPool;
                driver.commands = rpcBufferCommands;
                driver.bufferInstances = rpcBufferInstanceInputs;
                driver.ids = ids;

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

            public NativeParallelMultiHashMap<uint, RPCBufferInstance> bufferInstanceResults;

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
            private struct Command
            {
#if DEBUG
                public bool isActive;
#endif
                public BufferSegment bufferSegment;
                public NetworkPipeline pipeline;
                public IntPtr ptr;
            }

            private struct Driver : INetworkRPCDriver
            {
                public NetworkDriver.Concurrent instance;
                public NativeArray<RPCBuffer> bufferPool;
                public NativeArray<int> bufferPoolCount;
                public UnsafeList<Command> commands;
                public UnsafeHashMap<NetworkPipeline, RPCBuffer> buffers;

                public Driver(ref NetworkDriver.Concurrent instance, ref NativeArray<int> bufferPoolCount, in NativeArray<RPCBuffer> bufferPool)
                {
                    this.instance = instance;
                    this.bufferPool = bufferPool;
                    this.bufferPoolCount = bufferPoolCount;
                    commands = default;
                    buffers = default;
                }

                public void Dispose()
                {
                    if (commands.IsCreated)
                        commands.Dispose();

                    if (buffers.IsCreated)
                        buffers.Dispose();
                }

                public void Apply(uint id, ref NativeParallelMultiHashMap<uint, RPCBufferInstance>.ParallelWriter bufferInstances)
                {
                    if (buffers.IsCreated)
                    {
                        RPCBufferInstance bufferInstance;
                        foreach(var buffer in buffers)
                        {
                            bufferInstance.pipeline = buffer.Key;
                            bufferInstance.buffer = buffer.Value;
                            bufferInstances.Add(id, bufferInstance);
                        }

#if DEBUG
                        foreach (var command in commands)
                            UnityEngine.Assertions.Assert.IsFalse(command.isActive);
#endif
                    }
                }

                public RPCBuffer GetOrCreate(in NetworkPipeline pipeline, int capacity)
                {
                    if(!buffers.IsCreated)
                        buffers = new UnsafeHashMap<NetworkPipeline, RPCBuffer>(1, Allocator.Temp);

                    if (!buffers.TryGetValue(pipeline, out var buffer))
                    {
                        int bufferIndex = bufferPoolCount.Decrement(0);
                        if (bufferIndex < 0)
                        {
                            bufferPoolCount.Increment(0);

                            buffer = new RPCBuffer(capacity, Allocator.Persistent);
                        }
                        else
                        {
                            buffer = bufferPool[bufferIndex];

                            buffer.Clear();
                        }
                    }

                    return buffer;
                }

                public NetworkConnection.State GetConnectionState(in NetworkConnection connection)
                {
                    return instance.GetConnectionState(connection);
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

                    if (!buffers.IsCreated)
                    {
                        var statusCode = (StatusCode)instance.BeginSend(pipeline, connection, out writer, requiredPayloadSize);
                        switch (statusCode)
                        {
                            case StatusCode.Success:
                                command.bufferSegment = default;
                                command.ptr = writer.GetSendHandleData();

                                if (!commands.IsCreated)
                                    commands = new UnsafeList<Command>(1, Allocator.Temp);

                                commands.Add(command);

                                writer.SetSendHandleData((IntPtr)commands.Length);

                                return StatusCode.Success;
                            case StatusCode.NetworkSendQueueFull:
                                break;
                            default:
                                return statusCode;
                        }
                    }

                    command.bufferSegment.length = instance.PayloadCapacity(pipeline);

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

                    if (!commands.IsCreated)
                        commands = new UnsafeList<Command>(1, Allocator.Temp);

                    commands.Add(command);

                    writer = DataStreamUtility.CreateWriter(
                        buffer.value.writer.WriteBlock(command.bufferSegment.length, false).AsArray<byte>(),
                        (IntPtr)commands.Length);

                    buffers[pipeline] = buffer;

                    return StatusCode.Success;
                }

                public int EndSend(DataStreamWriter writer)
                {
                    int length = writer.Length, 
                        commandIndex = (int)writer.GetSendHandleData() - 1;
                    var command = commands[commandIndex];
#if DEBUG
                    if (length > NetworkParameterConstants.MTU)
                        Debug.LogWarning("Long Stream.");

                    UnityEngine.Assertions.Assert.IsTrue(command.isActive);

                    command.isActive = false;

                    commands[commandIndex] = command;
#endif

                    RPCBuffer buffer;
                    if (command.ptr == IntPtr.Zero)
                    {
#if DEBUG
                        writer.SetSendHandleData(IntPtr.Zero);
#endif

                        if (!buffers.TryGetValue(command.pipeline, out buffer))
                            return (int)StatusCode.NetworkSendHandleInvalid;

                        int position = command.bufferSegment.byteOffset + length;
                        if (buffer.value.length == command.bufferSegment.byteOffset + command.bufferSegment.length)
                            buffer.value.length = position;
                    }
                    else
                    {
                        writer.SetSendHandleData(command.ptr);

                        int result = instance.EndSend(writer);

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

                    buffers[command.pipeline] = buffer;

                    return length;
                }

                public void AbortSend(DataStreamWriter writer)
                {
                    int length = writer.Length,
                        commandIndex = (int)writer.GetSendHandleData() - 1;
                    var command = commands[commandIndex];
                    RPCBuffer buffer;
                    if (command.ptr == IntPtr.Zero)
                    {
                        if (!buffers.TryGetValue(command.pipeline, out buffer))
                            return;

                        int position = command.bufferSegment.byteOffset + length;
                        if (buffer.value.length == command.bufferSegment.byteOffset + command.bufferSegment.length)
                        {
                            buffer.value.length = position;

                            buffers[command.pipeline] = buffer;
                        }
                    }
                    else
                    {
                        writer.SetSendHandleData(command.ptr);

                        instance.AbortSend(writer);
                    }
                }
            }

            public StreamCompressionModel model;

            public NetworkRPCManager<int>.ReadOnly manager;

            public NetworkDriver.Concurrent driver;

            [ReadOnly]
            public NativeBuffer buffer;

            [ReadOnly]
            public NativeArray<RPCBuffer> rpcBufferPool;

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
            public NativeParallelMultiHashMap<uint, RPCBufferInstance> rpcBufferInstanceInputs;

            public NativeParallelMultiHashMap<uint, RPCBufferInstance>.ParallelWriter rpcBufferInstanceOutputs;

            [NativeDisableParallelForRestriction]
            public NativeArray<int> rpcBufferPoolCount;

            public void Execute(int index)
            {
                uint id = rpcIDs[index];
                var connection = manager.GetConnection(id);
                if (!connection.IsCreated)
                    return;

                var driver = new Driver(ref this.driver, ref rpcBufferPoolCount, rpcBufferPool);

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

            private void __CommandMain(uint id, in NetworkConnection connection, ref Driver driver, ref UnsafeHashMap<NetworkPipeline, DataStreamWriter> writers)
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

            private void __CommandInit(uint id, in NetworkConnection connection, ref Driver driver, ref UnsafeHashMap<NetworkPipeline, DataStreamWriter> writers)
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

            private void __CommandRPC(uint id, in NetworkConnection connection, ref Driver driver, ref UnsafeHashMap<NetworkPipeline, DataStreamWriter> writers)
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

        private NativeList<RPCBufferCommand> __rpcBufferCommands;

        private NativeList<RPCBuffer> __rpcBufferPool;

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

        private NativeParallelMultiHashMap<uint, RPCBufferInstance> __rpcBufferInstanceInputs;

        private NativeParallelMultiHashMap<uint, RPCBufferInstance> __rpcBufferInstanceOutputs;

        private NativeHashSet<uint> __idsToActive;

        private NativeHashSet<uint> __addIDs;

        private NativeHashSet<uint> __removeIDs;

        public NativeArray<ActiveEvent>.ReadOnly activeEventsReadOnly => __activeEvents.AsArray().AsReadOnly();

        public NetworkRPCCommander(in AllocatorManager.AllocatorHandle allocator)
        {
            __buffer = new NativeBuffer(allocator, 1);

            __rpcBufferCount = new NativeArray<int>(1, allocator.ToAllocator, NativeArrayOptions.ClearMemory);

            __activeEvents = new NativeList<ActiveEvent>(allocator);

            __rpcIDs = new NativeList<uint>(allocator);

            __sourceRPCCommands = new NativeList<RPCCommand>(allocator);

            __registerCommands = new NativeList<RegisterCommand>(allocator);
            __unregisterCommands = new NativeList<UnregisterCommand>(allocator);

            __rpcBufferCommands = new NativeList<RPCBufferCommand>(allocator);

            __rpcBufferPool = new NativeList<RPCBuffer>(allocator);

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

            __rpcBufferInstanceInputs = new NativeParallelMultiHashMap<uint, RPCBufferInstance>(1, allocator);
            __rpcBufferInstanceOutputs = new NativeParallelMultiHashMap<uint, RPCBufferInstance>(1, allocator);

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
            __rpcBufferCommands.Dispose();

            foreach (var rpcBuffer in __rpcBufferPool)
                rpcBuffer.Dispose();

            __rpcBufferPool.Dispose();

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
            in NetworkPipeline pipeline, 
            in NetworkDriver driver, 
            out DataStreamWriter stream)
        {
            int capacity = driver.PayloadCapacity(pipeline);
            //MSG TYPE
            capacity -= SizeOfCommandHeader;

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
            command.rpcBufferCommands = __rpcBufferCommands;
            command.rpcBufferPool = __rpcBufferPool;
            command.commandIndices = __commandIndices;
            command.moveCommands = __moveCommands;
            command.reconnectCommands = __reconnectCommands;
            command.types = __types;
            command.nodeDistances = __nodeDistances;
            command.versions = __versions;
            command.rpcBufferInstanceInputs = __rpcBufferInstanceInputs;
            command.rpcBufferInstanceOutputs = __rpcBufferInstanceOutputs;
            command.initEvents = __initEvents;
            command.idsToInit = __idsToInit;
            command.idsToActive = __idsToActive;
            command.addIDs = __addIDs;
            command.removeIDs = __removeIDs;
            command.manager = manager;

            return command.Schedule(inputDeps);
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
            var jobHandle = collectRPC.Schedule(inputDeps);

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
            command.rpcBufferPool = __rpcBufferPool.AsDeferredJobArray();
            command.rpcIDs = __rpcIDs.AsDeferredJobArray();
            command.versions = __versions;
            command.initCommands = __initCommands;
            command.initEvents = __initEvents;
            command.commands = __destinationRPCCommands;
            command.rpcBufferInstanceInputs = __rpcBufferInstanceInputs;
            command.rpcBufferInstanceOutputs = __rpcBufferInstanceOutputs.AsParallelWriter();
            command.rpcBufferPoolCount = __rpcBufferCount;

            jobHandle = command.Schedule(__rpcIDs, innerloopBatchCount, jobHandle);

            Clear clear;
            clear.buffer = __buffer;
            clear.initCommands = __initCommands;
            clear.initEvents = __initEvents;

            return clear.Schedule(jobHandle);
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