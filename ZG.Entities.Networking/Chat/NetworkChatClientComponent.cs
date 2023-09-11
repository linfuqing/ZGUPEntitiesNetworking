using Unity.Collections;
using Unity.Networking.Transport;
using UnityEngine;

namespace ZG
{
    public class NetworkChatClientComponent : MonoBehaviour
    {
        public delegate void Talk(uint id, ulong channel, in string name, DataStreamReader reader);

        public event Talk onTalk;

        [SerializeField]
        internal string _worldName = "Client";
        private NetworkChatClientManager __manager;

        private NativeList<ulong> __channels;
        private NativeList<NetworkChatClient.Message> __messages;

        public NetworkChatClient client
        {
            get
            {
                if (!__manager.isCreated)
                    __manager = WorldUtility.GetWorld(_worldName).GetOrCreateSystemUnmanaged<NetworkChatClientSystem>().client;

                __manager.lookupJobManager.CompleteReadWriteDependency();

                return __manager.client;
            }
        }

        public virtual void Shutdown()
        {
            client.Shutdown();
        }

        public virtual void Connect(in NetworkEndpoint endPoint)
        {
            client.Connect(endPoint);
        }

        protected void OnDestroy()
        {
            if(__channels.IsCreated)
                __channels.Dispose();

            if (__messages.IsCreated)
                __messages.Dispose();
        }

        protected void LateUpdate()
        {
            if(onTalk != null)
            {
                var client = this.client;

                if (__channels.IsCreated)
                    __channels.Clear();
                else
                    __channels = new NativeList<ulong>(Allocator.Persistent);

                client.GetChannels(ref __channels);

                foreach(ulong channel in __channels)
                {
                    if (__messages.IsCreated)
                        __messages.Clear();
                    else
                        __messages = new NativeList<NetworkChatClient.Message>(Allocator.Persistent);

                    client.GetMessages(channel, ref __messages);

                    foreach (var message in __messages)
                        onTalk(message.id, channel, message.name.ToString(), message.stream);
                }
            }
        }
    }
}