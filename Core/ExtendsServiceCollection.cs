using System;
using System.Reflection;
using LightestNight.System.EventSourcing.Checkpoints;
using LightestNight.System.EventSourcing.Events;
using LightestNight.System.EventSourcing.Persistence;
using LightestNight.System.EventSourcing.Replay;
using LightestNight.System.EventSourcing.SqlStreamStore.Checkpoints;
using LightestNight.System.EventSourcing.SqlStreamStore.Projections;
using LightestNight.System.EventSourcing.SqlStreamStore.Replay;
using LightestNight.System.EventSourcing.SqlStreamStore.Subscriptions;
using LightestNight.System.ServiceResolution;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using SqlStreamStore;

namespace LightestNight.System.EventSourcing.SqlStreamStore
{
    public static class ExtendsServiceCollection
    {
        /// <summary>
        /// Adds the core elements required for EventSourcing into the build in DI framework
        /// </summary>
        /// <param name="services">The <see cref="IServiceCollection" /> that contains all the registered services</param>
        /// <param name="optionsAccessor">An optional accessor containing <see cref="EventSourcingOptions" /></param>
        /// <param name="eventAssemblies">An optional collection of assemblies where to find the event types</param>
        /// <returns>The <see cref="IServiceCollection" /> populated with all the newly registered services</returns>
        public static IServiceCollection AddEventStore(this IServiceCollection services, Action<EventSourcingOptions>? optionsAccessor = null, params Assembly[] eventAssemblies)
        {
            AssemblyScanning.RegisterServices(services, new[]{Assembly.GetExecutingAssembly()}, new[]
            {
                new ConcreteRegistration
                {
                    InterfaceType = typeof(IEventSourceProjection),
                    AddIfAlreadyExists = true
                }
            });

            services.Configure(optionsAccessor);
            services.AddServiceResolution();
            services.TryAddSingleton<GetEventTypes>(() => EventCollection.GetEventTypes(eventAssemblies));
            services.TryAddSingleton<IReplayManager, ReplayManager>();
            services.TryAddSingleton<IEventPersistence, SqlEventStore>();
            return services.AddHostedService<EventSubscription>();
        }

        /// <summary>
        /// Adds the core elements required for EventSourcing using an in memory Event Store into the build in DI framework
        /// </summary>
        /// <param name="services">The <see cref="IServiceCollection" /> that contains all the registered services</param>
        /// <param name="optionsAccessor">An optional accessor containing <see cref="EventSourcingOptions" /></param>
        /// <param name="eventAssemblies">An optional collection of assemblies where to find the event types</param>
        /// <returns>The <see cref="IServiceCollection" /> populated with all the newly registered services</returns>
        public static IServiceCollection AddInMemoryEventStore(this IServiceCollection services,
            Action<EventSourcingOptions>? optionsAccessor = null, params Assembly[] eventAssemblies)
            => services.AddEventStore(optionsAccessor, eventAssemblies)
                .AddSingleton<IStreamStore, InMemoryStreamStore>()
                .AddSingleton<ICheckpointManager, CheckpointManager>();
    }
}