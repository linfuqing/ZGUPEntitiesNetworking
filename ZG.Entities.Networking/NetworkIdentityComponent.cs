using System;
using System.Reflection;
using System.Collections.Generic;
using Unity.Entities;
using Unity.Networking.Transport;
using UnityEngine;
using NetworkReader = Unity.Collections.DataStreamReader;
using NetworkWriter = Unity.Collections.DataStreamWriter;

namespace ZG
{
    public struct NetworkIdentityType : IComponentData
    {
        public int value;
    }

    [Serializable]
    public struct NetworkChannel
    {
        public NetworkPipelineType type;
    }

    public interface INetworkMessage
    {
        void Serialize(ref NetworkWriter writer);

        void Deserialize(ref NetworkReader reader);
    }

    public interface INetworkHost
    {
        bool BeginRPC(int channel, uint id, uint handle, out NetworkWriter writer);

        int EndRPC(NetworkWriter writer);

        //int RPC(int channel, uint id, uint handle);
    }


    public static partial class NetworkUtility
    {
        public static void WriteMessage<T>(this ref NetworkWriter writer, in T value) where T : INetworkMessage
        {
            value.Serialize(ref writer);
        }

        public static T ReadMessage<T>(this ref NetworkReader reader) where T : INetworkMessage, new()
        {
            T value = new T();
            value.Deserialize(ref reader);

            return value;
        }
    }

    [Serializable]
    public class NetworkHostTopology
    {
        public NetworkChannel[] channels;
    }

    [EntityComponent(typeof(NetworkIdentity))]
    [EntityComponent(typeof(NetworkIdentityType))]
    public class NetworkIdentityComponent : EntityProxyComponent
    {
        internal INetworkHost _host;

        internal Action _onCreate;
        internal Action _onDestroy;

        private Dictionary<uint, Action<NetworkConnection, NetworkReader>> __handlers;

        public bool isLocalPlayer
        {
            get;

            internal set;
        }

        public uint id
        {
            get;

            private set;
        }

        public int type
        {
            get => this.GetComponentData<NetworkIdentityType>().value;
        }

        public ref NetworkEntityManager networkEntityManager => ref world.GetOrCreateSystemUnmanaged<NetworkEntityManager>();

        public INetworkHost host
        {
            get
            {
                return _host;
            }
        }

        public event Action onCreate
        {
            add
            {
                _onCreate += value;
            }

            remove
            {
                _onCreate -= value;
            }
        }

        public event Action onDestroy
        {
            add
            {
                _onDestroy += value;
            }

            remove
            {
                _onDestroy -= value;
            }
        }

        public NetworkIdentityComponent Instantiate(
            in Quaternion rotation,
            in Vector3 position,
            int type,
            uint id, 
            bool isLocalPlayer)
        {
            ref var networkEntityManager = ref this.networkEntityManager;

            bool isExists = networkEntityManager.Exists(id);

            Entity prefab = networkEntityManager.Register(id);

            if (!isExists)
            {
                NetworkIdentityType networkIdentityType;
                networkIdentityType.value = type;

                networkEntityManager.factory.SetComponentData(prefab, networkIdentityType);
            }

            var instance = GameObjectEntity.Instantiate(this, null, position, rotation, prefab);

            instance.isLocalPlayer = isLocalPlayer;
            instance.id = id;

            return instance;
        }

        public bool InvokeHandler(uint handle, in NetworkConnection connection, NetworkReader reader)
        {
            if (__handlers == null)
                return false;

            if (!__handlers.TryGetValue(handle, out var action) || action == null)
            {
                Debug.LogError($"The handler of {handle} is missing!");

                return false;
            }

            try
            {
                UnityEngine.Profiling.Profiler.BeginSample($"{this}.InvokeHandler {handle}");
                action(connection, reader);
                UnityEngine.Profiling.Profiler.EndSample();
            }
            catch(Exception e)
            {
                UnityEngine.Debug.LogException(e.InnerException ?? e);
            }

            return true;
        }

        public bool UnregisterHandler(uint handle)
        {
            return __handlers != null && __handlers.Remove(handle);
        }

        public void RegisterHandler(uint handle, Action<NetworkConnection, NetworkReader> action)
        {
            if (__handlers == null)
                __handlers = new Dictionary<uint, Action<NetworkConnection, NetworkReader>>();

            __handlers.Add(handle, action);
        }

        public void RegisterHandler(uint handle, Action<NetworkReader> action)
        {
            RegisterHandler(handle, action == null ? (Action<NetworkConnection, NetworkReader>)null : delegate (NetworkConnection connection, NetworkReader reader)
            {
                action(reader);
            });
        }

        public int RPC(int channel, uint handle)
        {
            if (BeginRPC(channel, handle, out var writer))
                return EndRPC(writer);

            UnityEngine.Debug.LogError("[RPC]Fail.");

            return -1;
        }

        public bool BeginRPC(int channel, uint handle, out NetworkWriter writer)
        {
            if (_host == null)
            {
                writer = default;

                return false;
            }

            return _host.BeginRPC(channel, id, handle, out writer);
        }

        public int EndRPC(NetworkWriter writer)
        {
            return _host.EndRPC(writer);
        }

        public bool _Retain(uint id)
        {
            ref var networkEntityManager = ref this.networkEntityManager;

            if (!networkEntityManager.Retain(this.id, id))
                return false;

            this.id = id;

            return true;
        }
    }
}