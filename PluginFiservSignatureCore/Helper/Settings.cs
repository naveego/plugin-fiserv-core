using System;
using PluginFiservSignatureCore.API.Read;

namespace PluginFiservSignatureCore.Helper
{
    public class Settings
    {
        public string ConnectionString { get; set; }
        public string Password { get; set; }
        public bool? LightSync { get; set; }
        public RealTimeSettings SingleRealTimeSettings { get; set; }

        /// <summary>
        /// Validates the settings input object
        /// </summary>
        /// <exception cref="Exception"></exception>
        public void Validate()
        {
            if (String.IsNullOrEmpty(ConnectionString))
            {
                throw new Exception("the ConnectionString property must be set");
            }
        }

        public string GetConnectionString()
        {
            return ConnectionString.Replace("PASSWORD", Password);
        }

        public bool IsSingleRealTimeRead()
        {
            return SingleRealTimeSettings != null;
        }
    }
}