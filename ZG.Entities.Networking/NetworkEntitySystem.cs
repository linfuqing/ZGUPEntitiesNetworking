using Unity.Burst;
using Unity.Burst.Intrinsics;
using Unity.Collections;
using Unity.Entities;
using Unity.Jobs;
using Unity.Mathematics;

namespace ZG
{
    [BurstCompile, UpdateInGroup(typeof(PresentationSystemGroup))]
    public partial struct NetworkEntitySystem : ISystem
    {
        [BurstCompile]
        private struct DidChange : IJobChunk
        {
            public uint lastSystemVersion;

            [NativeDisableParallelForRestriction]
            public NativeArray<int> result;

            [ReadOnly]
            public ComponentTypeHandle<NetworkIdentity> identityType;

            public void Execute(in ArchetypeChunk chunk, int unfilteredChunkIndex, bool useEnabledMask, in v128 chunkEnabledMask)
            {
                if (chunk.DidChange(ref identityType, lastSystemVersion))
                    result[0] = 1;
            }
        }

        [BurstCompile]
        private struct Resize : IJob
        {
            [ReadOnly, DeallocateOnJobCompletion]
            public NativeArray<int> count;

            public NativeArray<int> result;

            public SharedHashMap<uint, Entity>.Writer entities;

            public void Execute()
            {
                bool result = this.result[0] != 0;
                int count = this.count[0];
                if (!result)
                {
                    result = entities.Count() != count;
                    if (result)
                        this.result[0] = 1;
                }

                if (result)
                {
                    entities.capacity = math.max(entities.capacity, count);
                    entities.Clear();
                }
            }
        }

        private struct RebuildEntites
        {
            [ReadOnly]
            public NativeArray<Entity> entityArray;
            [ReadOnly]
            public NativeArray<NetworkIdentity> identities;

            public SharedHashMap<uint, Entity>.ParallelWriter entities;

            public void Execute(int index)
            {
                entities.TryAdd(identities[index].id, entityArray[index]);
            }
        }

        [BurstCompile]
        private struct RebuildEntitesEx : IJobChunk
        {
            [ReadOnly, DeallocateOnJobCompletion]
            public NativeArray<int> result;

            [ReadOnly]
            public EntityTypeHandle entityType;

            [ReadOnly]
            public ComponentTypeHandle<NetworkIdentity> identityType;

            public SharedHashMap<uint, Entity>.ParallelWriter entities;

            public void Execute(in ArchetypeChunk chunk, int unfilteredChunkIndex, bool useEnabledMask, in v128 chunkEnabledMask)
            {
                if (result[0] == 0)
                    return;

                RebuildEntites rebuildEntites;
                rebuildEntites.entityArray = chunk.GetNativeArray(entityType);
                rebuildEntites.identities = chunk.GetNativeArray(ref identityType);
                rebuildEntites.entities = entities;

                var iterator = new ChunkEntityEnumerator(useEnabledMask, chunkEnabledMask, chunk.Count);
                while(iterator.NextEntityIndex(out int i))
                {
                    rebuildEntites.Execute(i);
                    //entities.TryAdd(identities[i].id, entityArray[i]);
                }
            }
        }

        private EntityQuery __group;

        private EntityTypeHandle __entityType;

        private ComponentTypeHandle<NetworkIdentity> __identityType;

        public SharedHashMap<uint, Entity> entities
        {
            get;

            private set;
        }

        public void OnCreate(ref SystemState state)
        {
            __group = state.GetEntityQuery(
                new EntityQueryDesc()
                {
                    All = new ComponentType[]
                    {
                        ComponentType.ReadOnly<NetworkIdentity>()
                    }, 
                    Options = EntityQueryOptions.IncludeDisabledEntities
                });

            __entityType = state.GetEntityTypeHandle();
            __identityType = state.GetComponentTypeHandle<NetworkIdentity>(true);

            entities = new SharedHashMap<uint, Entity>(Allocator.Persistent);
        }

        public void OnDestroy(ref SystemState state)
        {
            entities.Dispose();
        }

        [BurstCompile]
        public void OnUpdate(ref SystemState state)
        {
            var inputDeps = state.Dependency;

            var result = new NativeArray<int>(1, Allocator.TempJob, NativeArrayOptions.ClearMemory);
            var identityType = __identityType.UpdateAsRef(ref state);

            DidChange didChange;
            didChange.lastSystemVersion = state.LastSystemVersion;
            didChange.result = result;
            didChange.identityType = identityType;
            var jobHandle = didChange.ScheduleParallelByRef(__group, inputDeps);

            var count = new NativeArray<int>(1, Allocator.TempJob, NativeArrayOptions.ClearMemory);

            var entities = this.entities;
            ref var lookupJobManager = ref entities.lookupJobManager;

            lookupJobManager.CompleteReadWriteDependency();

            Resize resize;
            resize.count = count;
            resize.result = result;
            resize.entities = entities.writer;
            jobHandle = resize.ScheduleByRef(JobHandle.CombineDependencies(__group.CalculateEntityCountAsync(count, inputDeps), jobHandle));

            RebuildEntitesEx rebuildEntites;
            rebuildEntites.result = result;
            rebuildEntites.entityType = __entityType.UpdateAsRef(ref state);
            rebuildEntites.identityType = identityType;
            rebuildEntites.entities = entities.parallelWriter;

            jobHandle = rebuildEntites.ScheduleParallelByRef(__group, jobHandle);

            lookupJobManager.readWriteJobHandle = jobHandle;

            state.Dependency = jobHandle;
        }
    }
}