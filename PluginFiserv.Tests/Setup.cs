using System;

namespace PluginODBC
{
    public class Setup
    {
        private static bool _environmentSet = false;
        
        public static void EnsureEnvironment()
        {
            if (_environmentSet) return;

            var homePath = Environment.ExpandEnvironmentVariables("%HOME%");
            var packagePath = $"{homePath}/.nuget/packages/ibm.data.db2.core/1.3.0.100";
            var driverPath = $"{packagePath}/build/clidriver";
            var binPath = $"{driverPath}/bin";
            
            var currentPath = Environment.GetEnvironmentVariable("PATH");
            Environment.SetEnvironmentVariable("PATH", $"{binPath}:{currentPath}");
        }
    }
}