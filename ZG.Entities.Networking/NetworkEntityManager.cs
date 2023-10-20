using Unity.Jobs;
using Unity.Burst;
using Unity.Entities;
using Unity.Collections.LowLevel.Unsafe;

namespace ZG
{
    public struct NetworkIdentity : IComponentData
    {
        public uint id;

        /*public bool isLocalPlayer
        {
            get => IsLocalPlayer(value);

            set => this.value = SetLocalPlayer(type, value);
        }

        public int type => GetType(value);*/

        public static uint SetLocalPlayer(int type, bool value) => (uint)(type << 1) | (value ? 1u : 0u);

        public static bool IsLocalPlayer(uint value) => (value & 1u) == 1u;

        public static int GetType(uint value) => (int)(value >> 1);
    }

    [DisableAutoCreation, BurstCompile]
    public partial struct NetworkEntityManager : ISystem
    {
        private struct Identity
        {
            public uint originID;
            public int count;
            public Entity entity;
        }

        private UnsafeHashMap<uint, Identity> __identities;

        public EntityCommandFactory factory
        {
            get;

            private set;
        }

        public bool Exists(uint id)
        {
            return __identities.TryGetValue(id, out var identity) && factory.Exists(identity.entity);
        }

        public uint GetOriginID(uint id)
        {
            return __identities.TryGetValue(id, out var identity) ? identity.originID : 0;
        }

        public bool Retain(uint fromID, uint toID)
        {
            if(__identities.TryGetValue(toID, out var identity))
            {
                if (identity.originID == fromID)
                    return true;

                return false;
            }

            identity = __identities[fromID];
            if (identity.count != 0 || identity.originID != 0)
                return false;

            NetworkIdentity instance;
            instance.id = toID;
            factory.SetComponentData(identity.entity, instance);

            identity.originID = fromID;

            return __identities.TryAdd(toID, identity) && __identities.Remove(fromID);
        }

        public bool Release(uint id, bool isDestroy)
        {
            var identity = __identities[id];
            if (identity.count != 0 || !__identities.Remove(id))
                return false;

            if(isDestroy)
                factory.DestroyEntity(identity.entity);

            return true;
        }

        public Entity Register(uint id)
        {
            if (__identities.TryGetValue(id, out var identity))
                ++identity.count;
            else
            {
                var factory = this.factory;

                identity.entity = factory.CreateEntity();

                NetworkIdentity instance;
                instance.id = id;
                factory.SetComponentData(identity.entity, instance);

                identity.count = 1;
            }

            __identities[id] = identity;

            return identity.entity;
        }

        public bool Unregister(uint id)
        {
            if (!__identities.TryGetValue(id, out var identity))
                return false;

            if (--identity.count < 0)
                return false;

            if (identity.count == 0)
                identity.originID = 0;

            __identities[id] = identity;

            return true;
        }

        [BurstCompile]
        public void OnCreate(ref SystemState state)
        {
            factory = state.WorldUnmanaged.GetExistingSystemUnmanaged<EntityCommandFactorySystem>().factory;

            __identities = new UnsafeHashMap<uint, Identity>(1, Unity.Collections.Allocator.Persistent);
        }

        [BurstCompile]
        public void OnDestroy(ref SystemState state)
        {
            __identities.Dispose();
        }

        public void OnUpdate(ref SystemState state)
        {
            throw new System.NotImplementedException();
        }
    }
}