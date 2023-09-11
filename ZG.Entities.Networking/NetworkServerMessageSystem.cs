using System;
using System.Reflection;
using Unity.Burst;
using Unity.Collections;
using Unity.Entities;
using Unity.Jobs;
using Unity.Networking.Transport;

namespace ZG
{
    [AttributeUsage(AttributeTargets.Method)]
    public class NetworkServerMessageAttribute : Attribute
    {
        public uint type;
        public int handle;

        public int componentCount;
        public int bufferSize;
    }

    public delegate void NetworkServerMessageDelegate(
        in Entity entity, 
        in NetworkConnection connection, 
        ref DataStreamReader stream, 
        ref EntityComponentAssigner.ParallelWriter assigner);

    public struct NetworkServerMessageHandle : IEquatable<NetworkServerMessageHandle>
    {
        public uint type;
        public int handle;

        public bool Equals(NetworkServerMessageHandle other)
        {
            return type == other.type && handle == other.handle;
        }

        public override int GetHashCode()
        {
            return (int)type ^ handle;
        }
    }

    public struct NetworkServerMessageHandler
    {
        public int componentCount;
        public int bufferSize;
        public FunctionPointer<NetworkServerMessageDelegate> functionPointer;
    }

    public struct NetworkServerReceiveSystem
    {
        private struct Receive : IJobParallelForDefer
        {
            public StreamCompressionModel model;

            [ReadOnly]
            public NativeArray<uint> ids;

            [ReadOnly]
            public NetworkServerMessageManager manager;

            [ReadOnly]
            public SharedHashMap<uint, Entity>.Reader entities;

            [ReadOnly]
            public NativeHashMap<NetworkServerMessageHandle, NetworkServerMessageHandler> handlers;

            public EntityComponentAssigner.ParallelWriter assigner;

            public void Execute(int index)
            {
                uint id = ids[index];

                if (!entities.TryGetValue(id, out Entity entity))
                    return;

                var messages = new NativeList<NetworkServerMessage>(Allocator.Temp);

                if (manager.Receive(id, ref messages))
                {
                    DataStreamReader reader;
                    NetworkServerMessageHandle handle;
                    NetworkServerMessageHandler handler;
                    foreach (var message in messages)
                    {
                        reader = message.stream;
                        handle.handle = reader.ReadPackedInt(model);
                        handle.type = message.type;

                        if (!handlers.TryGetValue(handle, out handler))
                            continue;

                        handler.functionPointer.Invoke(entity, message.connection, ref reader, ref assigner);
                    }
                }

                messages.Dispose();
            }
        }
    }

    public static class NetworkServerMessageUtility
    {
        /*private class Message
        {
            public static ref FunctionPointer<NetworkServerMessageAttribute> GetFunctionPointer(Type delegateType)
            {
                return ref SharedStatic<FunctionPointer<NetworkServerMessageAttribute>>.GetOrCreate(typeof(Message), delegateType).Data;
            }
        }

        public class Message<T>
        {
            public readonly static SharedStatic<FunctionPointer<NetworkServerMessageAttribute>> FunctionPointer = SharedStatic<FunctionPointer<NetworkServerMessageAttribute>>.GetOrCreate<Message, T>();
        }*/

        public static void Create(ref NativeHashMap<NetworkServerMessageHandle, NetworkServerMessageHandler> handlers)
        {
            NetworkServerMessageHandle handle;
            NetworkServerMessageHandler handler;
            NetworkServerMessageAttribute attribute;
            foreach(var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                foreach(var type in assembly.GetTypes())
                {
                    if(type.IsClass && type.GetCustomAttribute<BurstCompileAttribute>() != null)
                    {
                        foreach(var method in type.GetMethods(BindingFlags.Static))
                        {
                            if(method.GetCustomAttribute<BurstCompileAttribute>() != null)
                            {
                                attribute = method.GetCustomAttribute<NetworkServerMessageAttribute>();
                                if(attribute != null)
                                {
                                    handle.type = attribute.type;
                                    handle.handle = attribute.handle;

                                    handler.componentCount = attribute.componentCount;
                                    handler.bufferSize = attribute.bufferSize;
                                    handler.functionPointer = BurstCompiler.CompileFunctionPointer((NetworkServerMessageDelegate)Delegate.CreateDelegate(typeof(NetworkServerMessageDelegate), method));

                                    handlers.Add(handle, handler);
                                }
                            }
                        }
                    }
                }
            }
            //NetworkServerMessageDelegate.CreateDelegate()
            //BurstCompiler.CompileFunctionPointer<NetworkServerMessageDelegate>
        }
    }
}