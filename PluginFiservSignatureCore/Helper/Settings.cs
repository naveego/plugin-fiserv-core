using System;

namespace PluginFiservSignatureCore.Helper
{
    public class Settings
    {
        public string Server { get; set; }
        
        public string Database { get; set; }

        public int Port { get; set; } = 50000;
        
        public string Username { get; set; }
        
        public string Password { get; set; }

        /// <summary>
        /// Validates the settings input object
        /// </summary>
        /// <exception cref="Exception"></exception>
        public void Validate()
        {
            if (String.IsNullOrEmpty(Server))
            {
                throw new Exception("The Server property must be set");
            }
            
            if (String.IsNullOrEmpty(Database))
            {
                throw new Exception("The Database property must be set");
            }
        }

        /// <summary>
        /// Gets the database connection string
        /// </summary>
        /// <returns></returns>
        public string GetConnectionString()
        {
            return $"SERVER={Server}:{Port};DATABASE={Database};UID={Username};PWD={Password}";
        }
        
        /// <summary>
        /// Gets the database connection string
        /// </summary>
        /// <returns></returns>
        public string GetConnectionString(string database)
        {
            return $"SERVER={Server}:{Port};DATABASE={database};UID={Username};PWD={Password}";
        }
    }
}