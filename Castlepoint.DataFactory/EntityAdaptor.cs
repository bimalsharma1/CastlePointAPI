using System;
using System.Collections.Generic;
using System.Text;
using System.ComponentModel;
using System.Linq;
using System.Reflection;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Castlepoint.DataFactory
{

    public abstract class EntityAdapter<T> : ITableEntity where T : class, new()
    {

        private T _value;

        protected EntityAdapter()
        {
        }

        protected EntityAdapter(T value)
        {
            _value = value;
        }

        /// <summary>
        ///     The synchronization lock.
        /// </summary>
        /// <remarks>A dictionary is not required here because the static will have a different value for each generic type.</remarks>
        private static readonly Object _syncLock = new Object();

        /// <summary>
        ///     The additional properties to map for types.
        /// </summary>
        /// <remarks>A dictionary is not required here because the static will have a different value for each generic type.</remarks>
        private static List<PropertyInfo> _additionalProperties;


        /// <inheritdoc />
        public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            _value = new T();

            // DEBUG
            if (_value.GetType()== typeof(POCO.RecordAssociationKeyPhraseCount))
            {
                bool isDebug = true;
            }

            TableEntity.ReadUserObject(Value, properties, operationContext);

            var additionalMappings = GetAdditionPropertyMappings(Value, properties);

            if (additionalMappings.Count > 0)
            {
                // Populate the properties missing from ReadUserObject
                foreach (var additionalMapping in additionalMappings)
                {
                    // FIX for PartitionKey and RowKey in the user object
                    if (properties.ContainsKey(additionalMapping.Name) == false
                        && (additionalMapping.Name!="PartitionKey"
                        && additionalMapping.Name!="RowKey"))
                    {
                        // We will let the object assign its default value for that property
                        continue;
                    }

                    switch(additionalMapping.Name)
                    {
                        case "PartitionKey":
                            additionalMapping.SetValue(Value, this.PartitionKey);
                            break;
                        case "RowKey":
                            additionalMapping.SetValue(Value, this.RowKey);
                            break;
                        default:
                            var propertyValue = properties[additionalMapping.Name];
                            var converter = TypeDescriptor.GetConverter(additionalMapping.PropertyType);
                            // NOT WORKING EITHER var convertedValue = converter.ConvertFrom(propertyValue.PropertyAsObject);
                            switch (propertyValue.PropertyType)
                            {
                                case EdmType.DateTime:
                                    if (additionalMapping.SetMethod != null) { additionalMapping.SetValue(Value, propertyValue.DateTime); }
                                    break;
                                case EdmType.Double:
                                    if (additionalMapping.SetMethod != null)
                                    {
                                        additionalMapping.SetValue(Value, propertyValue.DoubleValue);
                                    }
                                    break;
                                case EdmType.Int32:
                                    if (additionalMapping.SetMethod != null)
                                    {
                                        additionalMapping.SetValue(Value, propertyValue.Int32Value);
                                    }
                                    break;
                                case EdmType.Int64:
                                    if (additionalMapping.SetMethod != null)
                                    {
                                        additionalMapping.SetValue(Value, propertyValue.Int64Value);
                                    }
                                    break;
                                case EdmType.Boolean:
                                    if (additionalMapping.SetMethod != null)
                                    {
                                        additionalMapping.SetValue(Value, propertyValue.BooleanValue);
                                    }
                                    break;
                                case EdmType.String:
                                    if (additionalMapping.PropertyType==typeof(DateTime))
                                    {
                                        DateTime convertedDateTime;
                                        bool isConvereted = DateTime.TryParse(propertyValue.StringValue, out convertedDateTime);
                                        if (isConvereted)
                                        {
                                            if (additionalMapping.SetMethod != null)
                                            { additionalMapping.SetValue(Value, convertedDateTime); }
                                        }
                                        else
                                        {
                                            var convertedDateTimeValue = converter.ConvertFromInvariantString(propertyValue.StringValue);
                                            if (additionalMapping.SetMethod != null)
                                            { additionalMapping.SetValue(Value, convertedDateTimeValue); }
                                        }
                                    }
                                    else
                                    {
                                        var convertedStringValue = converter.ConvertFromInvariantString(propertyValue.StringValue);
                                        if (additionalMapping.SetMethod != null)
                                        { additionalMapping.SetValue(Value, convertedStringValue); }
                                    }
                                    break;
                                default:
                                    var convertedValue = converter.ConvertFromInvariantString(propertyValue.StringValue);
                                    if (additionalMapping.SetMethod != null)
                                    { additionalMapping.SetValue(Value, convertedValue); }
                                    break;
                            }

                            break;
                    }
                }
            }


            ReadValues(properties, operationContext);
        }


        /// <inheritdoc />
        public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var properties = TableEntity.WriteUserObject(Value, operationContext);

            var additionalMappings = GetAdditionPropertyMappings(Value, properties);

            if (additionalMappings.Count > 0)
            {
                // Populate the properties missing from WriteUserObject
                foreach (var additionalMapping in additionalMappings)
                {
                    var propertyValue = additionalMapping.GetValue(Value);
                    var converter = TypeDescriptor.GetConverter(additionalMapping.PropertyType);
                    var convertedValue = converter.ConvertToInvariantString(propertyValue);

                    properties[additionalMapping.Name] = EntityProperty.GeneratePropertyForString(convertedValue);
                }
            }

            WriteValues(properties, operationContext);

            return properties;
        }



        protected virtual void ReadValues(
            IDictionary<string, EntityProperty> properties,
            OperationContext operationContext)
        {
        }

        protected virtual void WriteValues(
            IDictionary<string, EntityProperty> properties,
            OperationContext operationContext)
        {
        }





        /// <summary>
        ///     Gets the additional property mappings.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="properties">The mapped properties.</param>
        /// <returns>
        ///     The additional property mappings.
        /// </returns>
        private static List<PropertyInfo> GetAdditionPropertyMappings(
            T value,
            IDictionary<string, EntityProperty> properties)
        {
            if (_additionalProperties != null)
            {
                return _additionalProperties;
            }

            List<PropertyInfo> additionalProperties;

            lock (_syncLock)
            {
                // Check the mappings again to protect against race conditions on the lock
                if (_additionalProperties != null)
                {
                    return _additionalProperties;
                }

                additionalProperties = ResolvePropertyMappings(value, properties);

                _additionalProperties = additionalProperties;
            }

            return additionalProperties;
        }

        /// <summary>
        ///     Resolves the additional property mappings.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="properties">The properties.</param>
        /// <returns>The additional properties.</returns>
        private static List<PropertyInfo> ResolvePropertyMappings(
            T value,
            IDictionary<string, EntityProperty> properties)
        {
            var objectProperties = value.GetType().GetProperties();

            return
                objectProperties.Where(objectProperty => properties.ContainsKey(objectProperty.Name) == false).ToList();
        }

        /// <inheritdoc />
        public string ETag
        {
            get;
            set;
        }

        /// <summary>
        ///     The partition key
        /// </summary>
        private string _partitionKey;

        /// <summary>
        ///     The row key
        /// </summary>
        private string _rowKey;

        /// <inheritdoc />
        public string PartitionKey
        {
            get
            {
                if (_partitionKey == null)
                {
                    _partitionKey = BuildPartitionKey();
                }

                return _partitionKey;
            }

            set
            {
                _partitionKey = value;
            }
        }

        /// <summary>
        ///     Builds the entity partition key.
        /// </summary>
        /// <returns>
        ///     The partition key of the entity.
        /// </returns>
        protected abstract string BuildPartitionKey();

        /// <summary>
        ///     Builds the entity row key.
        /// </summary>
        /// <returns>
        ///     The <see cref="string" />.
        /// </returns>
        protected abstract string BuildRowKey();


        /// <inheritdoc />
        public string RowKey
        {
            get
            {
                if (_rowKey == null)
                {
                    _rowKey = BuildRowKey();
                }

                return _rowKey;
            }

            set
            {
                _rowKey = value;
            }
        }

        /// <inheritdoc />
        public DateTimeOffset Timestamp
        {
            get;
            set;
        }

        public T Value
        {
            get
            {
                return _value;
            }
        }

    }

}
