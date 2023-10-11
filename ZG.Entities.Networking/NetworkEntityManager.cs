using Unity.Jobs;
using Unity.Burst;
using Unity.Entities;
using Unity.Collections.LowLevel.Unsafe;

namespace ZG
{
    [DisableAutoCreation, BurstCompile]
    public struct NetworkEntityManager : ISystem
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

        public Entity GetEntity(uint id)
        {
            return __identities.TryGetValue(id, out var identity) ? factory.GetEntity(identity.entity) : Entity.Null;
        }

        public uint GetOriginID(uint id)
        {
            return __identities.TryGetValue(id, out var identity) ? identity.originID : 0;
        }

        public bool Change(uint fromID, uint toID)
        {
            var identity = __identities[fromID];
            identity.originID = fromID;

            return __identities.TryAdd(toID, identity) && __identities.Remove(fromID);
        }

        public Entity Register(uint id)
        {
            if (__identities.TryGetValue(id, out var identity))
                ++identity.count;
            else
            {
                identity.entity = factory.CreateEntity();

                identity.count = 1;
            }

            __identities[id] = identity;

            return identity.entity;
        }

        public bool Unregister(uint id)
        {
            if (!__identities.TryGetValue(id, out var identity))
                return false;

            if (--identity.count < 1)
            {
                factory.DestroyEntity(identity.entity);

                __identities.Remove(id);
            }
            else
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