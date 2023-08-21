using System.Collections;
using System.Collections.Generic;
using Unity.Burst;
using Unity.Collections;
using Unity.Entities;
using Unity.Networking.Transport;

namespace ZG
{
    public delegate void NetworkServerMessageDelegate(
        in Entity entity, 
        in NetworkConnection connection, 
        ref DataStreamReader stream, 
        ref EntityComponentAssigner assigner);

    public struct NetworkServerMessageSystem
    {
    }

    public static class NetworkServerMessageUtility
    {
        public static void BurstAll()
        {
            //NetworkServerMessageDelegate.CreateDelegate()
            //BurstCompiler.CompileFunctionPointer<NetworkServerMessageDelegate>
        }
    }
}