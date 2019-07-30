using System.Configuration;

namespace Pql.IntegrationStubs
{
    public class WindowsServiceConfigSection : ConfigurationSection
    {
        [ConfigurationProperty("windowsServiceName", DefaultValue = "mycompanyServiceHost", IsRequired = false)]
        [StringValidator(InvalidCharacters = " ~!@#$%^&*()[]{}/;'\"|\\", MinLength = 5, MaxLength = 100)]
        public string WindowsServiceName
        {
            get
            {
                return (string)this["windowsServiceName"];
            }
            set
            {
                this["windowsServiceName"] = value;
            }
        }

        [ConfigurationProperty("windowsServiceDisplayName", DefaultValue = "mycompanyServiceHost", IsRequired = false)]
        [StringValidator(InvalidCharacters = "~!@#$%^&*()[]{}/;'\"|\\", MinLength = 5, MaxLength = 100)]
        public string WindowsServiceDisplayName
        {
            get
            {
                return (string)this["windowsServiceDisplayName"];
            }
            set
            {
                this["windowsServiceDisplayName"] = value;
            }
        }

        [ConfigurationProperty("windowsServiceDescription", DefaultValue = "mycompanyServiceHost", IsRequired = false)]
        [StringValidator(InvalidCharacters = "~!@#$%^&*()[]{}/;'\"|\\", MinLength = 5, MaxLength = 100)]
        public string WindowsServiceDescription
        {
            get
            {
                return (string)this["windowsServiceDescription"];
            }
            set
            {
                this["windowsServiceDescription"] = value;
            }
        }
    }
}
