using System;
using PluginFiservSignatureCore.Helper;
using Xunit;

namespace PluginFiservSignatureCore.Tests.Helper
{
    public class SettingsTest
    {
        [Fact]
        public void ValidateValidTest()
        {
            // setup
            var settings = new Settings
            {
                
            };

            // act
            settings.Validate();

            // assert
        }

        [Fact]
        public void ValidateNoTNSNameTest()
        {
            // setup
            var settings = new Settings
            {
                
            };

            // act
            Exception e = Assert.Throws<Exception>(() => settings.Validate());

            // assert
            Assert.Contains("The TNSName property must be set", e.Message);
        }
        
        [Fact]
        public void ValidateNoWalletPathTest()
        {
            // setup
            var settings = new Settings
            {
                
            };

            // act
            Exception e = Assert.Throws<Exception>(() => settings.Validate());

            // assert
            Assert.Contains("The WalletPath property must be set", e.Message);
        }

        
        
        [Fact]
        public void GetConnectionStringTest()
        {
            // setup
            var settings = new Settings
            {
                
            };

            // act
            var connString = settings.GetConnectionString();

            // assert
            Assert.Equal("testtns", connString);
        }
    }
}