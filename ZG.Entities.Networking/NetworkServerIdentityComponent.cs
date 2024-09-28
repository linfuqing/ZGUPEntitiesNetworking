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

        public NetworkServerEntityComponentFlag flag;

        public NetworkServerComponentAttribute(
            uint handle,
            Type componentType,
            Type additionalIDComponentType,
            Type maskIDComponentType,
            int channel = 0,
            int rpcType = (int)NetworkRPCType.Normal, 
            NetworkServerEntityComponentFlag flag = 0)
        {
            this.componentType = componentType;
            this.additionalIDComponentType = additionalIDComponentType;
            this.maskIDComponentType = maskIDComponentType;
            this.rpcType = rpcType;
            this.channel = channel;
            this.handle = handle;
            this.flag = flag;
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
                if (__Visit(transform, x => x.componentType != null))
                    return new[]
                    {
                        typeof(NetworkServerEntityComponent),
                        typeof(NetworkServerEntityID),
                        typeof(NetworkServerEntityBufferRange),
                        typeof(NetworkServerEntityBuffer)
                    };


                return null;
            }
        }

        void IEntityComponent.Init(in Entity entity, EntityComponentAssigner assigner)
        {
            var targetEntity = entity;
            var world = base.world.Unmanaged;
            __Visit(transform, x =>
            {
                if (x.componentType == null)
                    return false;

                NetworkServerEntityComponent entityComponent;
                entityComponent.flag = x.flag;
                entityComponent.componentTypeIndex = NetworkServerEntitySystem.GetOrCreateComponentTypeIndex(
                    world,
                    TypeManager.GetTypeIndex(x.componentType));

                if (x.additionalIDComponentType == null)
                    entityComponent.additionalIDComponentTypeIndex = -1;
                else
                    entityComponent.additionalIDComponentTypeIndex =
                        NetworkServerEntitySystem.GetOrCreateComponentTypeIndex(world,
                            TypeManager.GetTypeIndex(x.additionalIDComponentType));

                if (x.maskIDComponentType == null)
                    entityComponent.maskIDComponentTypeIndex = -1;
                else
                    entityComponent.maskIDComponentTypeIndex =
                        NetworkServerEntitySystem.GetOrCreateComponentTypeIndex(world,
                            TypeManager.GetTypeIndex(x.maskIDComponentType));

                entityComponent.rpcType = x.rpcType;
                entityComponent.handle = x.handle;
                entityComponent.channel = x.channel;

                assigner.SetBuffer(EntityComponentAssigner.BufferOption.Append, targetEntity, entityComponent);

                return false;
            });
        }
        
        private static bool __Visit(Transform transform, Predicate<NetworkServerComponentAttribute> predicate)
        {
            if (__components == null)
                __components = new List<Component>();
            else
                __components.Clear();

            transform.GetComponents(__components);
            IEnumerable<NetworkServerComponentAttribute> componentAttributes;
            Type type;
            foreach (var component in __components)
            {
                type = component == null ? null : component.GetType();
                while (type != null && type != typeof(MonoBehaviour) && type != typeof(Behaviour))
                {
                    componentAttributes = type.GetCustomAttributes<NetworkServerComponentAttribute>(false);
                    foreach (var componentAttribute in componentAttributes)
                    {
                        if (predicate(componentAttribute))
                            return true;
                    }

                    type = type.BaseType;
                }
            }

            foreach (Transform child in transform)
            {
                if(child.GetComponent<IEntityComponentRoot>() != null)
                    continue;

                if (__Visit(child, predicate))
                    return true;
            }

            return false;
        }

    }
}