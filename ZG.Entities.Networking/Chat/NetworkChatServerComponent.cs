using Unity.Networking.Transport;
using UnityEngine;

namespace ZG
{
    public class NetworkChatServerComponent : MonoBehaviour
    {
        [SerializeField]
        internal string _worldName = "Server";
        private NetworkChatServerManager __manager;

        public bool isListening => server.isListening;

        public NetworkChatServer server
        {
            get
            {
                if (!__manager.isCreated)
                    __manager = WorldUtility.GetWorld(_worldName).GetOrCreateSystemUnmanaged<NetworkChatServerSystem>().manager;

                __manager.lookupJobManager.CompleteReadWriteDependency();

                return __manager.server;
            }
        }

        public void Listen(ushort port, NetworkFamily family = NetworkFamily.Ipv4)
        {
            server.Listen(port, family);
        }
    }
}