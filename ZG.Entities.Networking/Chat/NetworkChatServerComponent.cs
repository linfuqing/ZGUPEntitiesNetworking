using Unity.Networking.Transport;
using UnityEngine;

namespace ZG
{
    public class NetworkChatServerComponent : MonoBehaviour
    {
        [SerializeField]
        internal string _worldName = "Server";
        private NetworkChatServerSystem __system;

        public bool isListening => system.isListening;

        public NetworkChatServerSystem system
        {
            get
            {
                if (__system == null)
                    __system = WorldUtility.GetOrCreateWorld(_worldName).GetOrCreateSystemManaged<NetworkChatServerSystem>();

                return __system;
            }
        }

        public void Listen(ushort port, NetworkFamily family = NetworkFamily.Ipv4)
        {
            system.Listen(port, family);
        }
    }
}