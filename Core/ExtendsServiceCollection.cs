using System;
using LightestNight.System.EventSourcing.Checkpoints;
using LightestNight.System.EventSourcing.Persistence;
using LightestNight.System.EventSourcing.Replay;
using LightestNight.System.EventSourcing.SqlStreamStore.Checkpoints;
using LightestNight.System.EventSourcing.SqlStreamStore.Replay;
using LightestNight.System.EventSourcing.SqlStreamStore.Serialization;
using LightestNight.System.ServiceResolution;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using SqlStreamStore;

namespace LightestNight.System.EventSourcing.SqlStreamStore
{
    public static class ExtendsServiceCollection
    {
        /// <summary>
        /// Adds the core elements required for EventSourcing into the build in DI framework
        /// </summary>
        /// <param name="services">The <see cref="IServiceCollection" /> that contains all the registered services</param>
        /// <param name="customSerializer">The custom serializer to use when serializing and deserializing messages</param>
        /// <param name="eventSourcingOptionsAccessor">An optional accessor containing <see cref="EventSourcingOptions" /></param>
        /// <returns>The <see cref="IServiceCollection" /> populated with all the newly registered services</returns>
        public static IServiceCollection AddEventStore(this IServiceCollection services, ISerializer? customSerializer = null, Action<EventSourcingOptions>? eventSourcingOptionsAccessor = null)
        {
            if (customSerializer != null)
                SerializerFactory.SetSerializer(customSerializer);
            
            services.AddServiceResolution();
            services.TryAddSingleton(sp =>
            {
                var logger = sp.GetService<ILogger>();
                logger?.LogDebug("Adding Event Sourcing Options");
                
                var options = new EventSourcingOptions();
                eventSourcingOptionsAccessor?.Invoke(options);

                return options;
            });
            services.TryAddSingleton<IReplayManager, ReplayManager>();
            services.TryAddSingleton<IEventPersistence, SqlEventStore>();
            return services;
            //return services.AddHostedService<EventSubscription>();
        }

        /// <summary>
        /// Adds the core elements required for EventSourcing using an in memory Event Store into the build in DI framework
        /// </summary>
        /// <param name="services">The <see cref="IServiceCollection" /> that contains all the registered services</param>
        /// <param name="customSerializer">The custom serializer to use when serializing and deserializing messages</param>
        /// <param name="optionsAccessor">An optional accessor containing <see cref="EventSourcingOptions" /></param>
        /// <returns>The <see cref="IServiceCollection" /> populated with all the newly registered services</returns>
        public static IServiceCollection AddInMemoryEventStore(this IServiceCollection services, ISerializer? customSerializer = null,
            Action<EventSourcingOptions>? optionsAccessor = null)
        {
            services.AddEventStore(customSerializer, optionsAccessor)
                .AddSingleton<IStreamStore, InMemoryStreamStore>();

            services.TryAddSingleton<GetGlobalCheckpoint>(_ => CheckpointManager.GetGlobalCheckpoint);
            services.TryAddSingleton<SetGlobalCheckpoint>(_ => CheckpointManager.SetGlobalCheckpoint);

            return services;
        }
    }
}