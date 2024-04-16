using System;
using System.Reflection;
using System.Collections.Generic;
using Unity.Entities;
using UnityEngine;

namespace ZG
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true, Inherited = false)]
    public class NetworkServerComponentAttribute : Attribute
    {
        public Type componentType;
        public Type additionalIDComponentType;
        public Type maskIDComponentType;
        public int rpcType;
        public int channel;
        public uint handle;

        public NetworkServerComponentAttribute(
            uint handle,
            Type componentType,
            Type additionalIDComponentType,
            Type maskIDComponentType,
            int channel = 0,
            int rpcType = -1)
        {
            this.componentType = componentType;
            this.additionalIDComponentType = additionalIDComponentType;
            this.maskIDComponentType = maskIDComponentType;
            this.rpcType = rpcType;
            this.channel = channel;
            this.handle = handle;
        }
    }

    public class NetworkServerIdentityComponent : EntityProxyComponent, IEntityComponent
    {
        private static List<Component> __components;

        [EntityComponents]
        public Type[] componentTypes
        {
            get
            {
                if (__components == null)
                    __components = new List<Component>();
                else
                    __components.Clear();

                transform.GetComponentsInChildren<Component>(true, __components.Add, typeof(IEntityComponentRoot));

                Type type;
                IEnumerable<NetworkServerComponentAttribute> componentAttributes;
                foreach (var component in __components)
                {
                    type = component == null ? null : component.GetType();
                    while (type != null && type != typeof(MonoBehaviour) && type != typeof(Behaviour))
                    {
                        componentAttributes = type.GetCustomAttributes<NetworkServerComponentAttribute>(false);
                        foreach (var componentAttribute in componentAttributes)
                        {
                            if (componentAttribute.componentType == null)
                                continue;

                            return new[]
                            {
                                typeof(NetworkServerEntityComponent),
                                typeof(NetworkServerEntityID),
                                typeof(NetworkServerEntityBufferRange),
                                typeof(NetworkServerEntityBuffer)
                            };
                        }

                        type = type.BaseType;
                    }
                }

                return null;
            }
        }
        
        void IEntityComponent.Init(in Entity entity, EntityComponentAssigner assigner)
        {
            if (__components == null)
                __components = new List<Component>();
            else
                __components.Clear();

            transform.GetComponentsInChildren<Component>(true, __components.Add, typeof(IEntityComponentRoot));
            
            var world = base.world.Unmanaged;
            NetworkServerEntityComponent entityComponent;
            Type type;
            IEnumerable<NetworkServerComponentAttribute> serverComponentAttributes;
            foreach (var component in __components)
            {
                type = component == null ? null : component.GetType();
                while (type != null && type != typeof(MonoBehaviour) && type != typeof(Behaviour))
                {
                    serverComponentAttributes = type.GetCustomAttributes<NetworkServerComponentAttribute>(false);
                    foreach (var networkComponentAttribute in serverComponentAttributes)
                    {
                        entityComponent.componentTypeIndex = NetworkServerEntitySystem.GetOrCreateComponentTypeIndex(
                            world,
                            TypeManager.GetTypeIndex(networkComponentAttribute.componentType));

                        if (networkComponentAttribute.additionalIDComponentType == null)
                            entityComponent.additionalIDComponentTypeIndex = -1;
                        else
                            entityComponent.additionalIDComponentTypeIndex =
                                NetworkServerEntitySystem.GetOrCreateComponentTypeIndex(world,
                                    TypeManager.GetTypeIndex(networkComponentAttribute.additionalIDComponentType));

                        if (networkComponentAttribute.maskIDComponentType == null)
                            entityComponent.maskIDComponentTypeIndex = -1;
                        else
                            entityComponent.maskIDComponentTypeIndex =
                                NetworkServerEntitySystem.GetOrCreateComponentTypeIndex(world,
                                    TypeManager.GetTypeIndex(networkComponentAttribute.maskIDComponentType));

                        entityComponent.rpcType = networkComponentAttribute.rpcType;
                        entityComponent.handle = networkComponentAttribute.handle;
                        entityComponent.channel = networkComponentAttribute.channel;
                        
                        assigner.SetBuffer(EntityComponentAssigner.BufferOption.Append, entity, entityComponent);
                    }

                    type = type.BaseType;
                }
            }
        }
    }
}