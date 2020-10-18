using System;

using McMaster.Extensions.CommandLineUtils;
using System.Security.Cryptography.X509Certificates;

namespace Castlepoint.Configure
{
    class Program
    {
        public static void Main(string[] args)
        {
            var app = new CommandLineApplication();

            app.HelpOption("-h|--help");

            var optionAction = app.Option("-a|--action", "Action to perform, can be one of: installcert", CommandOptionType.SingleValue);
            var optionOS = app.Option("-os|--os", "The operating system, either Windows or Ubuntu", CommandOptionType.SingleValue);
            var optionCertPath = app.Option("-p|--certpath", "The path to the certificate", CommandOptionType.SingleValue);
            var optionCertPassword = app.Option("-pwd|--certpassword", "(optional) The certificate password", CommandOptionType.SingleValue);
            var optionWindowsCertStoreName = app.Option("-sn|--certstorename", "The Certificate Store Name, either CurrentUser or LocalMachine", CommandOptionType.SingleValue);
            var optionWindowsCertStoreLocation = app.Option("-sl|--certstorelocation", "The Certificate Store location, must be one of: [AddressBook, AuthRoot, CertificateAuthority, Disallowed, My, Root, TrustedPeople, TrustedPublisher]", CommandOptionType.SingleValue);

            app.OnExecute(() =>
            {
                var oAction = optionAction.HasValue()
                    ? optionAction.Value()
                    : string.Empty;

                var oOS = optionOS.HasValue()
                    ? optionOS.Value()
                    : string.Empty;

                var oCertPath = optionCertPath.HasValue()
                    ? optionCertPath.Value()
                    : string.Empty;

                var oCertPassword = optionCertPassword.HasValue()
                    ? optionCertPassword.Value()
                    : string.Empty;

                var oCertWinStore = optionWindowsCertStoreName.HasValue()
                                    ? optionWindowsCertStoreName.Value()
                                    : string.Empty;

                var oCertWinStoreLocation = optionWindowsCertStoreLocation.HasValue()
                    ? optionWindowsCertStoreLocation.Value()
                    : string.Empty;

                switch (oAction)
                {
                    case "installcert":

                        // Check the OS we are installing on
                        switch (oOS)
                        {
                            case "windows":
                                InstallCertificateOnWindows(oCertPath, oCertPassword, oCertWinStore, oCertWinStoreLocation);
                                break;
                            default:
                                Console.WriteLine("This OS is not currently supported by the tool: " + oOS);
                                break;
                        }

                        break;
                    default:
                        Console.WriteLine("ERROR: please provide an Action to perform.");
                        break;
                }

                Console.WriteLine("Castlepoint Configure finished");
 
                return 0;
            });

            app.Execute(args);

            return;
        }

        private static void InstallCertificateOnWindows(string certPath, string certPassword, string certStoreName, string certStoreLocation)
        {
            // Check the file path
            if (!System.IO.File.Exists(certPath))
            {
                Console.WriteLine("Could not find certificate at path: " + certPath);
                return;
            }

            // Default vars
            var storeLocation = System.Security.Cryptography.X509Certificates.StoreLocation.CurrentUser;
            var storeName = System.Security.Cryptography.X509Certificates.StoreName.My;


            switch (certStoreLocation)
            {
                case "LocalMachine":
                    Console.WriteLine("Store Location set to: " + certStoreLocation);
                    storeLocation = System.Security.Cryptography.X509Certificates.StoreLocation.LocalMachine;
                    break;
                case "CurrentUser":
                    Console.WriteLine("Store Location set to: " + certStoreLocation);
                    storeLocation = System.Security.Cryptography.X509Certificates.StoreLocation.CurrentUser;
                    break;
                default:
                    Console.WriteLine("WARNING Store Location not found: " + certStoreLocation);
                    return;
            }
            // Check the store name and location provided
            switch (certStoreName)
            {
                case "AddressBook":
                    Console.WriteLine("Store Name set to: " + certStoreName);
                    storeName = System.Security.Cryptography.X509Certificates.StoreName.AddressBook;
                    break;
                case "AuthRoot":
                    Console.WriteLine("Store Name set to: " + certStoreName);
                    storeName = System.Security.Cryptography.X509Certificates.StoreName.AuthRoot;
                    break;
                case "CertificateAuthority":
                    Console.WriteLine("Store Name set to: " + certStoreName);
                    storeName = System.Security.Cryptography.X509Certificates.StoreName.CertificateAuthority;
                    break;
                case "Disallowed":
                    Console.WriteLine("Store Name set to: " + certStoreName);
                    storeName = System.Security.Cryptography.X509Certificates.StoreName.Disallowed;
                    break;
                case "My":
                    Console.WriteLine("Store Name set to: " + certStoreName);
                    storeName = System.Security.Cryptography.X509Certificates.StoreName.My;
                    break;
                case "Root":
                    Console.WriteLine("Store Name set to: " + certStoreName);
                    storeName = System.Security.Cryptography.X509Certificates.StoreName.Root;
                    break;
                case "TrustedPeople":
                    Console.WriteLine("Store Name set to: " + certStoreName);
                    storeName = System.Security.Cryptography.X509Certificates.StoreName.TrustedPeople;
                    break;
                    Console.WriteLine("Store Name set to: " + certStoreName);
                case "TrustedPublisher":
                    storeName = System.Security.Cryptography.X509Certificates.StoreName.TrustedPublisher;
                    Console.WriteLine("Store Name set to: " + certStoreName);
                    break;
                default:
                    Console.WriteLine("WARNING Store Name not found: " + certStoreName);
                    return;
            }


            // Load the certificate
            try
            {
                Console.WriteLine("Loading certificate bytes from: " + certPath);
                var certBytes = System.IO.File.ReadAllBytes(certPath);

                Console.WriteLine("Opening store...");
                using (var certificate = new X509Certificate2(certBytes, certPassword, X509KeyStorageFlags.PersistKeySet))
                using (var store = new X509Store(storeName, storeLocation, OpenFlags.ReadWrite))
                {
                    Console.WriteLine("Attempting to add Certificate...");
                    store.Add(certificate);
                    Console.WriteLine("Closing store...");
                    store.Close();
                    Console.WriteLine("Certificate added.");
                }
            }
            catch (Exception exLoadCert)
            {
                Console.WriteLine("ERROR occurred while installing certificate from: " + certPath + " [" + exLoadCert.Message + "]");
            }


            return;
        }
    }
}
