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
        int size { get; }
        
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

    /*public struct NetworkHandlerManager
    {
        private enum MethodType
        {
            Normal,
            ReadOnly
        }

        private struct Method
        {
            public MethodType type;

            public int actionIndex;
        }

        private class Shared
        {
            private Pool<Action<NetworkConnection, NetworkReader>> __actions;
            private Pool<Action<NetworkReader>> __actionsReadOnly;
            private Dictionary<(object, uint), Method> __methods;
            
            public bool InvokeHandler(object identity, uint handle, in NetworkConnection connection, NetworkReader reader)
            {
                if (__methods == null)
                    return false;

                if (!__methods.TryGetValue((identity, handle), out var method))
                {
                    Debug.LogError($"The handler of {handle} is missing!");

                    return false;
                }

                switch (method.type)
                {
                    case MethodType.ReadOnly:
                        if (!__actionsReadOnly.TryGetValue(method.actionIndex, out var actionReadOnly))
                        {
                            Debug.LogError($"The handler of {handle} is missing!");

                            return false;
                        }
                        
                        try
                        {
#if ENABLE_PROFILER
                            UnityEngine.Profiling.Profiler.BeginSample(actionReadOnly.Target.GetType().Name);
                            UnityEngine.Profiling.Profiler.BeginSample(actionReadOnly.Method.Name);
#endif
                            actionReadOnly(reader);
                
#if ENABLE_PROFILER
                            UnityEngine.Profiling.Profiler.EndSample();
                            UnityEngine.Profiling.Profiler.EndSample();
#endif
                        }
                        catch(Exception e)
                        {
                            UnityEngine.Debug.LogException(e.InnerException ?? e);
                        }

                        break;
                    default:
                        if (!__actions.TryGetValue(method.actionIndex, out var action))
                        {
                            Debug.LogError($"The handler of {handle} is missing!");

                            return false;
                        }
                        
                        try
                        {
#if ENABLE_PROFILER
                            UnityEngine.Profiling.Profiler.BeginSample(action.Target.GetType().Name);
                            UnityEngine.Profiling.Profiler.BeginSample(action.Method.Name);
#endif
                            action(connection, reader);
                
#if ENABLE_PROFILER
                            UnityEngine.Profiling.Profiler.EndSample();
                            UnityEngine.Profiling.Profiler.EndSample();
#endif
                        }
                        catch(Exception e)
                        {
                            UnityEngine.Debug.LogException(e.InnerException ?? e);
                        }
                        break;
                }

                return true;
            }

            public bool UnregisterHandler(object identity, uint handle)
            {
                if (__methods == null || !__methods.TryGetValue((identity, handle), out var method))
                    return false;
                
                switch (method.type)
                {
                    case MethodType.ReadOnly:
                        if (!__actionsReadOnly.RemoveAt(method.actionIndex))
                            return false;
                        break;
                    default:
                        if (!__actions.RemoveAt(method.actionIndex))
                            return false;

                        break;
                }

                return true;
            }

            public void RegisterHandler(object identity, uint handle, Action<NetworkConnection, NetworkReader> action)
            {
                if (__actions == null)
                    __actions = new Pool<Action<NetworkConnection, NetworkReader>>();

                Method method;
                method.type = MethodType.Normal;
                method.actionIndex = __actions.Add(action);
                
                if (__methods == null)
                    __methods = new Dictionary<(object, uint), Method>();

                __methods.Add((identity, handle), method);
            }

            public void RegisterHandler(object identity, uint handle, Action<NetworkReader> action)
            {
                if (__actionsReadOnly == null)
                    __actionsReadOnly = new Pool<Action<NetworkReader>>();

                Method method;
                method.type = MethodType.ReadOnly;
                method.actionIndex = __actionsReadOnly.Add(action);
                
                if (__methods == null)
                    __methods = new Dictionary<(object, uint), Method>();

                __methods.Add((identity, handle), method);
            }

        }
        
        private static Shared __shared;

        private object __instance;
    }*/

    [EntityComponent(typeof(NetworkIdentity))]
    [EntityComponent(typeof(NetworkIdentityType))]
    public class NetworkIdentityComponent : EntityProxyComponent
    {
        internal INetworkHost _host;

        internal Action _onCreate;
        internal Action _onDestroy;

        private Dictionary<uint, Delegate> __handlers;

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

            if (!__handlers.TryGetValue(handle, out var handler) || handler == null)
            {
                Debug.LogError($"The handler of {handle} is missing!");

                return false;
            }

            try
            {
#if ENABLE_PROFILER
                UnityEngine.Profiling.Profiler.BeginSample(handler.Target.GetType().Name);
                UnityEngine.Profiling.Profiler.BeginSample(handler.Method.Name);
#endif
                var action = handler as Action<NetworkConnection, NetworkReader>;
                if(action != null)
                    action(connection, reader);
                else
                    ((Action<NetworkReader>)handler)(reader);
                
#if ENABLE_PROFILER
                UnityEngine.Profiling.Profiler.EndSample();
                UnityEngine.Profiling.Profiler.EndSample();
#endif
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
                __handlers = new Dictionary<uint, Delegate>();

            __handlers.Add(handle, action);
        }

        public void RegisterHandler(uint handle, Action<NetworkReader> action)
        {
            if (__handlers == null)
                __handlers = new Dictionary<uint, Delegate>();

            __handlers.Add(handle, action);
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