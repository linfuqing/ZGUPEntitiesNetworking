using System;
using System.Collections.Generic;
using Unity.Entities;
using Unity.Networking.Transport;
using UnityEngine;
using NetworkReader = Unity.Collections.DataStreamReader;
using NetworkWriter = Unity.Collections.DataStreamWriter;

namespace ZG
{
    public struct NetworkIdentity : IComponentData
    {
        public uint id;
        public uint value;

        public bool isLocalPlayer
        {
            get => IsLocalPlayer(value);

            set => this.value = SetLocalPlayer(type, value);
        }

        public int type => GetType(value);

        public static uint SetLocalPlayer(int type, bool value) => (uint)(type << 1) | (value ? 1u : 0u);

        public static bool IsLocalPlayer(uint value) => (value & 1u) == 1u;

        public static int GetType(uint value) => (int)(value >> 1);
    }

    [Serializable]
    public struct NetworkChannel
    {
        public NetworkPipelineType type;
    }

    [Serializable]
    public class NetworkHostTopology
    {
        public NetworkChannel[] channels;
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

    [EntityComponent(typeof(NetworkIdentity))]
    public class NetworkIdentityComponent : EntityProxyComponent
    {
        internal INetworkHost _host;

        internal Action _onCreate;
        internal Action _onDestroy;

        private Dictionary<uint, Action<NetworkConnection, NetworkReader>> __handlers;

        public bool isLocalPlayer
        {
            get
            {
                return identity.isLocalPlayer;
            }

            internal set
            {
                var identity = this.identity;
                identity.isLocalPlayer = value;

                this.SetComponentData(identity);
            }
        }

        public int type
        {
            get
            {
                return identity.type;
            }
        }

        public uint id
        {
            get
            {
                return identity.id;
            }
        }

        public NetworkIdentity identity => this.GetComponentData<NetworkIdentity>();

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
            uint id, 
            uint value)
        {
            ref var manager = ref world.GetOrCreateSystemUnmanaged<NetworkEntityManager>();

            Entity prefab = manager.Register(id);
            NetworkIdentity identity;
            identity.id = id;
            identity.value = value;
            manager.factory.SetComponentData(prefab, identity);

            return GameObjectEntity.Instantiate(this, null, position, rotation, prefab);
        }

        public bool InvokeHandler(uint handle, in NetworkConnection connection, NetworkReader reader)
        {
            if (__handlers == null)
                return false;

            if (!__handlers.TryGetValue(handle, out var action) || action == null)
            {
                UnityEngine.Debug.LogError($"The handler of {handle} is missing!");

                return false;
            }

            try
            {
                action(connection, reader);
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
    }
}