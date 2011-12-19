using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.IO;

namespace SynchronizationServer
{
    public class ThreadPoolTcpSrvr
    {
        private TcpListener client;
        int numOfClients;
        int port;
        ConnectionThread newconnection;

        public ThreadPoolTcpSrvr(int numberOfClients, int port)
        {
            numOfClients = numberOfClients;
            this.port = port;
        }

        ~ThreadPoolTcpSrvr()
        {
            client.Stop();
        }

        public void Start()
        {
            IPHostEntry ihe = Dns.GetHostByName(Dns.GetHostName());
            IPAddress myself = ihe.AddressList[0];

            Console.WriteLine("Listening for clients at: " + myself.ToString() + ":" + port + "...");
            client = new TcpListener(myself, port);
            try
            {
                client.Start();
            }
            catch(SocketException e)
            {
                Console.WriteLine("Socket Exception, restarting...");
                return;
            }

            Console.WriteLine("Waiting for clients...");
            int i = 0;
            newconnection = new ConnectionThread();
            newconnection.threadListener = this.client;

            while (i++ < numOfClients)
            {
                while (!client.Pending())
                {
                    Thread.Sleep(1000);
                }
                newconnection.HandleConnection(null);
            }

            newconnection.ExecuteCommandSynchronized("metad");
            newconnection.ExecuteCommandSynchronized("mkdir");
            newconnection.ExecuteCommandSynchronized("cpfl_");
            newconnection.ExecuteCommandSynchronized("cptl_");
            newconnection.ExecuteCommandSynchronized("rmr__");
            newconnection.ExecuteCommandSynchronized("exit_");
            newconnection.killThreads();
        }

        public void killThreads()
        {
            newconnection.killThreads();
        }

        public static void Main(String[] args)
        {
            while (true)
            {
                try
                {
                    ThreadPoolTcpSrvr tpts = null;
                    if (args.Length == 1)
                        tpts = new ThreadPoolTcpSrvr(Convert.ToInt32((args[0])), 9050);
                    else
                        tpts = new ThreadPoolTcpSrvr(1, 9050);

                    tpts.Start();
                }
                catch(Exception e){}
            }
        }
    }

    class ConnectionThread
    {
        public TcpListener threadListener;

        public class Client
        {
            public TcpClient client;
            public NetworkStream ns;
            public String latestMessageRead = "";
        }
        List<Client> cList = new List<Client>();
        List<Thread> cThread = new List<Thread>();

        ~ConnectionThread()
        {
            for (int i = 0; i < cList.Count; i++)
            {
                cList[i].ns.Close();
                cList[i].client.Close();
            }
        }

        public void HandleConnection(object state)
        {
            TcpClient client = threadListener.AcceptTcpClient();
            NetworkStream ns = client.GetStream();

            Client cl = new Client();
            cl.client = client;
            cl.ns = ns;
            cList.Add(cl);

            Console.WriteLine("New client accepted: " + client.Client.RemoteEndPoint);
 
            Thread clThread = new Thread(this.ReadRemoteClient);
            cThread.Add(clThread);
            clThread.Start(cl);
        }

        public void ReadRemoteClient(object messageObj)
        {
            Client cl = (Client)messageObj;
            Console.WriteLine("Reading from client: " + cl.client.Client.LocalEndPoint);
            while(true)
            {
                byte[] data = new byte[5];
                try
                {
                    int recv = cl.ns.Read(data, 0, data.Length);
                    String message = Encoding.ASCII.GetString(data);
                    if (message != "\r\n\0\0\0")
                    {
                        cl.latestMessageRead = message;
                        Console.WriteLine("String read from " + cl.client.Client.RemoteEndPoint + ": " + message);
                    }
                }
                catch(IOException e)
                {
                    Console.WriteLine("IOException occured. Exiting reading thread.");
                    cl.latestMessageRead = "threadDead";
                    break;
                }
            }
        }

        public void ExecuteCommandSynchronized(String command)
        {
            byte[] data = Encoding.ASCII.GetBytes(command);

            for(int  i=0; i<cList.Count; i++)
            {
                cList[i].ns.Write(data, 0, data.Length);
            }

            if (command.CompareTo("exit_") == 0)
                return;

            while(true)
            {
                bool check = true;
                for(int i=0; i<cList.Count; i++)
                {
                    if (cList[i].latestMessageRead.CompareTo("threadDead") == 0)
                        break;

                    check = (cList[i].latestMessageRead.CompareTo(command) == 0) & check;
                }

                if (check)
                {
                    Console.WriteLine("Command completed: " + command);
                    break;
                }

                Console.WriteLine("Waiting for command to complete: " + command);
                Thread.Sleep(1000);
            }
        }

        public void killThreads()
        {
            //close threads
            for (int i = 0; i < cThread.Count; i++)
                cThread[i].Abort();
        }
    }
}
