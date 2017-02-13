// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Sockets;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Linq.Expressions;
using System.ComponentModel;
using Microsoft.Extensions.Internal;

namespace Microsoft.AspNetCore.SignalR
{
    public class HubEndPoint<THub> : HubEndPoint<THub, IClientProxy> where THub : Hub<IClientProxy>
    {
        public HubEndPoint(HubLifetimeManager<THub> lifetimeManager,
                           IHubContext<THub> hubContext,
                           InvocationAdapterRegistry registry,
                           ILogger<HubEndPoint<THub>> logger,
                           IServiceScopeFactory serviceScopeFactory)
            : base(lifetimeManager, hubContext, registry, logger, serviceScopeFactory)
        {
        }
    }

    public class HubEndPoint<THub, TClient> : EndPoint, IInvocationBinder where THub : Hub<TClient>
    {
        private readonly Dictionary<string, HubMethodDescriptor> _methods = new Dictionary<string, HubMethodDescriptor>(StringComparer.OrdinalIgnoreCase);

        private readonly HubLifetimeManager<THub> _lifetimeManager;
        private readonly IHubContext<THub, TClient> _hubContext;
        private readonly ILogger<HubEndPoint<THub, TClient>> _logger;
        private readonly InvocationAdapterRegistry _registry;
        private readonly IServiceScopeFactory _serviceScopeFactory;

        public HubEndPoint(HubLifetimeManager<THub> lifetimeManager,
                           IHubContext<THub, TClient> hubContext,
                           InvocationAdapterRegistry registry,
                           ILogger<HubEndPoint<THub, TClient>> logger,
                           IServiceScopeFactory serviceScopeFactory)
        {
            _lifetimeManager = lifetimeManager;
            _hubContext = hubContext;
            _registry = registry;
            _logger = logger;
            _serviceScopeFactory = serviceScopeFactory;

            DiscoverHubMethods();
        }

        public override async Task OnConnectedAsync(Connection connection)
        {
            try
            {
                await _lifetimeManager.OnConnectedAsync(connection);
                await RunHubAsync(connection);
            }
            finally
            {
                await _lifetimeManager.OnDisconnectedAsync(connection);
            }
        }

        private async Task RunHubAsync(Connection connection)
        {
            await HubOnConnectedAsync(connection);

            try
            {
                await DispatchMessagesAsync(connection);
            }
            catch (Exception ex)
            {
                _logger.LogError(0, ex, "Error when processing requests.");
                await HubOnDisconnectedAsync(connection, ex);
                throw;
            }

            await HubOnDisconnectedAsync(connection, null);
        }

        private async Task HubOnConnectedAsync(Connection connection)
        {
            try
            {
                using (var scope = _serviceScopeFactory.CreateScope())
                {
                    var hubActivator = scope.ServiceProvider.GetRequiredService<IHubActivator<THub, TClient>>();
                    var hub = hubActivator.Create();
                    try
                    {
                        InitializeHub(hub, connection);
                        await hub.OnConnectedAsync();
                    }
                    finally
                    {
                        hubActivator.Release(hub);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(0, ex, "Error when invoking OnConnectedAsync on hub.");
                throw;
            }
        }

        private async Task HubOnDisconnectedAsync(Connection connection, Exception exception)
        {
            try
            {
                using (var scope = _serviceScopeFactory.CreateScope())
                {
                    var hubActivator = scope.ServiceProvider.GetRequiredService<IHubActivator<THub, TClient>>();
                    var hub = hubActivator.Create();
                    try
                    {
                        InitializeHub(hub, connection);
                        await hub.OnDisconnectedAsync(exception);
                    }
                    finally
                    {
                        hubActivator.Release(hub);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(0, ex, "Error when invoking OnDisconnectedAsync on hub.");
                throw;
            }
        }

        private async Task DispatchMessagesAsync(Connection connection)
        {
            var invocationAdapter = _registry.GetInvocationAdapter(connection.Metadata.Get<string>("formatType"));

            // We use these for error handling. Since we dispatch multiple hub invocations
            // in parallel, we need a way to communicate failure back to the main processing loop. The
            // cancellation token is used to stop reading from the channel, the tcs
            // is used to get the exception so we can bubble it up the stack
            var cts = new CancellationTokenSource();
            var tcs = new TaskCompletionSource<object>();

            try
            {
                while (await connection.Transport.Input.WaitToReadAsync(cts.Token))
                {
                    Message incomingMessage;
                    while (connection.Transport.Input.TryRead(out incomingMessage))
                    {
                        InvocationDescriptor invocationDescriptor;
                        using (incomingMessage)
                        {
                            var inputStream = new MemoryStream(incomingMessage.Payload.Buffer.ToArray());

                            // TODO: Handle receiving InvocationResultDescriptor
                            invocationDescriptor = await invocationAdapter.ReadMessageAsync(inputStream, this) as InvocationDescriptor;
                        }

                        // Is there a better way of detecting that a connection was closed?
                        if (invocationDescriptor == null)
                        {
                            break;
                        }

                        if (_logger.IsEnabled(LogLevel.Debug))
                        {
                            _logger.LogDebug("Received hub invocation: {invocation}", invocationDescriptor);
                        }

                        // Don't wait on the result of execution, continue processing other
                        // incoming messages on this connection.
                        var ignore = ProcessInvocation(connection, invocationAdapter, invocationDescriptor, cts, tcs);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Await the task so the exception bubbles up to the caller
                await tcs.Task;
            }
        }

        private async Task ProcessInvocation(Connection connection,
                                             IInvocationAdapter invocationAdapter,
                                             InvocationDescriptor invocationDescriptor,
                                             CancellationTokenSource cts,
                                             TaskCompletionSource<object> tcs)
        {
            try
            {
                // If an unexpected exception occurs then we want to kill the entire connection
                // by ending the processing loop
                await Execute(connection, invocationAdapter, invocationDescriptor);
            }
            catch (Exception ex)
            {
                // Set the exception on the task completion source
                tcs.TrySetException(ex);

                // Cancel reading operation
                cts.Cancel();
            }
        }

        private async Task Execute(Connection connection, IInvocationAdapter invocationAdapter, InvocationDescriptor invocationDescriptor)
        {
            InvocationResultDescriptor result;
            HubMethodDescriptor descriptor;
            if (_methods.TryGetValue(invocationDescriptor.Method, out descriptor))
            {
                result = await Invoke(descriptor, connection, invocationDescriptor);
            }
            else
            {
                // If there's no method then return a failed response for this request
                result = new InvocationResultDescriptor
                {
                    Id = invocationDescriptor.Id,
                    Error = $"Unknown hub method '{invocationDescriptor.Method}'"
                };

                _logger.LogError("Unknown hub method '{method}'", invocationDescriptor.Method);
            }

            // TODO: Pool memory
            var outStream = new MemoryStream();
            await invocationAdapter.WriteMessageAsync(result, outStream);

            var buffer = ReadableBuffer.Create(outStream.ToArray()).Preserve();
            var outMessage = new Message(buffer, Format.Text, endOfMessage: true);

            while (await connection.Transport.Output.WaitToWriteAsync())
            {
                if (connection.Transport.Output.TryWrite(outMessage))
                {
                    break;
                }
            }
        }

        private async Task<InvocationResultDescriptor> Invoke(HubMethodDescriptor descriptor, Connection connection, InvocationDescriptor invocationDescriptor)
        {
            var invocationResult = new InvocationResultDescriptor
            {
                Id = invocationDescriptor.Id
            };

            var methodInfo = descriptor.MethodInfo;

            using (var scope = _serviceScopeFactory.CreateScope())
            {
                var hubActivator = scope.ServiceProvider.GetRequiredService<IHubActivator<THub, TClient>>();
                var hub = hubActivator.Create();

                try
                {
                    InitializeHub(hub, connection);

                    var executor = ObjectMethodExecutor.Create(methodInfo, typeof(THub).GetTypeInfo());
                    var res = executor.MethodReturnType;

                    if (executor.IsMethodAsync)
                    {
                        await executor.ExecuteAsync(hub, invocationDescriptor.Arguments);
                    }
                    var result = methodInfo.Invoke(hub, invocationDescriptor.Arguments);
                    var resultTask = result as Task;
                    if (resultTask != null)
                    {
                        await resultTask;
                        if (methodInfo.ReturnType.GetTypeInfo().IsGenericType)
                        {
                            var property = resultTask.GetType().GetProperty("Result");
                            invocationResult.Result = property?.GetValue(resultTask);
                        }
                    }
                    else
                    {
                        invocationResult.Result = result;
                    }
                }
                catch (TargetInvocationException ex)
                {
                    _logger.LogError(0, ex, "Failed to invoke hub method");
                    invocationResult.Error = ex.InnerException.Message;
                }
                catch (Exception ex)
                {
                    _logger.LogError(0, ex, "Failed to invoke hub method");
                    invocationResult.Error = ex.Message;
                }
                finally
                {
                    hubActivator.Release(hub);
                }
            }

            return invocationResult;
        }

        private void InitializeHub(THub hub, Connection connection)
        {
            hub.Clients = _hubContext.Clients;
            hub.Context = new HubCallerContext(connection);
            hub.Groups = new GroupManager<THub>(connection, _lifetimeManager);
        }

        private void DiscoverHubMethods()
        {
            var type = typeof(THub);

            foreach (var methodInfo in type.GetTypeInfo().DeclaredMethods.Where(m => IsHubMethod(m)))
            {
                var methodName = methodInfo.Name;

                if (_methods.ContainsKey(methodName))
                {
                    throw new NotSupportedException($"Duplicate definitions of '{methodInfo.Name}'. Overloading is not supported.");
                }

                _methods[methodName] = new HubMethodDescriptor(methodInfo);

                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.LogDebug("Hub method '{methodName}' is bound", methodName);
                }
            };
        }

        private static bool IsHubMethod(MethodInfo methodInfo)
        {
            // TODO: Add more checks
            if (!methodInfo.IsPublic || methodInfo.IsSpecialName)
            {
                return false;
            }

            var baseDefinition = methodInfo.GetBaseDefinition().DeclaringType;
            var baseType = baseDefinition.GetTypeInfo().IsGenericType ? baseDefinition.GetGenericTypeDefinition() : baseDefinition;
            if (typeof(Hub<>) == baseType)
            {
                return false;
            }

            return true;
        }

        Type IInvocationBinder.GetReturnType(string invocationId)
        {
            return typeof(object);
        }

        Type[] IInvocationBinder.GetParameterTypes(string methodName)
        {
            HubMethodDescriptor descriptor;
            if (!_methods.TryGetValue(methodName, out descriptor))
            {
                return Type.EmptyTypes;
            }
            return descriptor.ParameterTypes;
        }

        // REVIEW: We can decide to move this out of here if we want pluggable hub discovery
        private class HubMethodDescriptor
        {
            public HubMethodDescriptor(MethodInfo methodInfo)
            {
                MethodInfo = methodInfo;
                ParameterTypes = methodInfo.GetParameters().Select(p => p.ParameterType).ToArray();
            }

            public MethodInfo MethodInfo { get; }

            public Type[] ParameterTypes { get; }
        }
    }

    public class ObjectMethodExecutor
    {
        private object[] _parameterDefaultValues;
        private ActionExecutorAsync _executorAsync;
        private ActionExecutor _executor;

        private static readonly MethodInfo _convertOfTMethod =
            typeof(ObjectMethodExecutor).GetRuntimeMethods().Single(methodInfo => methodInfo.Name == nameof(ObjectMethodExecutor.Convert));

        private ObjectMethodExecutor(MethodInfo methodInfo, TypeInfo targetTypeInfo)
        {            
            if (methodInfo == null)
            {
                throw new ArgumentNullException(nameof(methodInfo));
            }
            MethodInfo = methodInfo;
            TargetTypeInfo = targetTypeInfo;
            ActionParameters = methodInfo.GetParameters();
            MethodReturnType = methodInfo.ReturnType;
            IsMethodAsync = typeof(Task).IsAssignableFrom(MethodReturnType);
            TaskGenericType = IsMethodAsync ? GetTaskInnerTypeOrNull(MethodReturnType) : null;
            //IsTypeAssignableFromIActionResult = typeof(IActionResult).IsAssignableFrom(TaskGenericType ?? MethodReturnType);
        }

        private delegate Task<object> ActionExecutorAsync(object target, object[] parameters);

        private delegate object ActionExecutor(object target, object[] parameters);

        private delegate void VoidActionExecutor(object target, object[] parameters);

        public MethodInfo MethodInfo { get; }

        public ParameterInfo[] ActionParameters { get; }

        public TypeInfo TargetTypeInfo { get; }

        public Type TaskGenericType { get; }

        // This field is made internal set because it is set in unit tests.
        public Type MethodReturnType { get; internal set; }

        public bool IsMethodAsync { get; }

        public bool IsTypeAssignableFromIActionResult { get; }

        private ActionExecutorAsync TaskOfTActionExecutorAsync
        {
            get
            {
                if (_executorAsync == null)
                {
                    _executorAsync = GetExecutorAsync(TaskGenericType, MethodInfo, TargetTypeInfo);
                }

                return _executorAsync;
            }
        }

        public static ObjectMethodExecutor Create(MethodInfo methodInfo, TypeInfo targetTypeInfo)
        {
            var executor = new ObjectMethodExecutor(methodInfo, targetTypeInfo);
            executor._executor = GetExecutor(methodInfo, targetTypeInfo);
            return executor;
        }

        public Task<object> ExecuteAsync(object target, object[] parameters)
        {
            return TaskOfTActionExecutorAsync(target, parameters);
        }

        public object Execute(object target, object[] parameters)
        {
            return _executor(target, parameters);
        }

        public object GetDefaultValueForParameter(int index)
        {
            if (index < 0 || index > ActionParameters.Length - 1)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            EnsureParameterDefaultValues();

            return _parameterDefaultValues[index];
        }

        private static ActionExecutor GetExecutor(MethodInfo methodInfo, TypeInfo targetTypeInfo)
        {
            // Parameters to executor
            var targetParameter = Expression.Parameter(typeof(object), "target");
            var parametersParameter = Expression.Parameter(typeof(object[]), "parameters");

            // Build parameter list
            var parameters = new List<Expression>();
            var paramInfos = methodInfo.GetParameters();
            for (int i = 0; i < paramInfos.Length; i++)
            {
                var paramInfo = paramInfos[i];
                var valueObj = Expression.ArrayIndex(parametersParameter, Expression.Constant(i));
                var valueCast = Expression.Convert(valueObj, paramInfo.ParameterType);

                // valueCast is "(Ti) parameters[i]"
                parameters.Add(valueCast);
            }

            // Call method
            MethodCallExpression methodCall;

            if (!methodInfo.IsStatic)
            {
                var instanceCast = Expression.Convert(targetParameter, targetTypeInfo.AsType());
                methodCall = Expression.Call(instanceCast, methodInfo, parameters);
            }
            else
            {
                methodCall = Expression.Call(null, methodInfo, parameters);
            }

            // methodCall is "((Ttarget) target) method((T0) parameters[0], (T1) parameters[1], ...)"
            // Create function
            if (methodCall.Type == typeof(void))
            {
                var lambda = Expression.Lambda<VoidActionExecutor>(methodCall, targetParameter, parametersParameter);
                var voidExecutor = lambda.Compile();
                return WrapVoidAction(voidExecutor);
            }
            else
            {
                // must coerce methodCall to match ActionExecutor signature
                var castMethodCall = Expression.Convert(methodCall, typeof(object));
                var lambda = Expression.Lambda<ActionExecutor>(castMethodCall, targetParameter, parametersParameter);
                return lambda.Compile();
            }
        }

        private static ActionExecutor WrapVoidAction(VoidActionExecutor executor)
        {
            return delegate (object target, object[] parameters)
            {
                executor(target, parameters);
                return null;
            };
        }

        private static ActionExecutorAsync GetExecutorAsync(Type taskInnerType, MethodInfo methodInfo, TypeInfo targetTypeInfo)
        {
            // Parameters to executor
            var targetParameter = Expression.Parameter(typeof(object), "target");
            var parametersParameter = Expression.Parameter(typeof(object[]), "parameters");

            // Build parameter list
            var parameters = new List<Expression>();
            var paramInfos = methodInfo.GetParameters();
            for (int i = 0; i < paramInfos.Length; i++)
            {
                var paramInfo = paramInfos[i];
                var valueObj = Expression.ArrayIndex(parametersParameter, Expression.Constant(i));
                var valueCast = Expression.Convert(valueObj, paramInfo.ParameterType);

                // valueCast is "(Ti) parameters[i]"
                parameters.Add(valueCast);
            }

            // Call method
            var instanceCast = Expression.Convert(targetParameter, targetTypeInfo.AsType());
            var methodCall = Expression.Call(instanceCast, methodInfo, parameters);

            var coerceMethodCall = GetCoerceMethodCallExpression(taskInnerType, methodCall, methodInfo);
            var lambda = Expression.Lambda<ActionExecutorAsync>(coerceMethodCall, targetParameter, parametersParameter);
            return lambda.Compile();
        }

        // We need to CoerceResult as the object value returned from methodInfo.Invoke has to be cast to a Task<T>.
        // This is necessary to enable calling await on the returned task.
        // i.e we need to write the following var result = await (Task<ActualType>)mInfo.Invoke.
        // Returning Task<object> enables us to await on the result.
        private static Expression GetCoerceMethodCallExpression(
            Type taskValueType,
            MethodCallExpression methodCall,
            MethodInfo methodInfo)
        {
            var castMethodCall = Expression.Convert(methodCall, typeof(object));
            // for: public Task<T> Action()
            // constructs: return (Task<object>)Convert<T>((Task<T>)result)
            var genericMethodInfo = _convertOfTMethod.MakeGenericMethod(taskValueType);
            var genericMethodCall = Expression.Call(null, genericMethodInfo, castMethodCall);
            var convertedResult = Expression.Convert(genericMethodCall, typeof(Task<object>));
            return convertedResult;
        }

        /// <summary>
        /// Cast Task of T to Task of object
        /// </summary>
        private static async Task<object> CastToObject<T>(Task<T> task)
        {
            return (object)await task;
        }

        private static Type GetTaskInnerTypeOrNull(Type type)
        {
            var genericType = ClosedGenericMatcher.ExtractGenericInterface(type, typeof(Task<>));

            return genericType?.GenericTypeArguments[0];
        }

        private static Task<object> Convert<T>(object taskAsObject)
        {
            var task = (Task<T>)taskAsObject;
            return CastToObject<T>(task);
        }

        private void EnsureParameterDefaultValues()
        {
            if (_parameterDefaultValues == null)
            {
                var count = ActionParameters.Length;
                _parameterDefaultValues = new object[count];

                for (var i = 0; i < count; i++)
                {
                    var parameterInfo = ActionParameters[i];
                    object defaultValue;

                    if (parameterInfo.HasDefaultValue)
                    {
                        defaultValue = parameterInfo.DefaultValue;
                    }
                    else
                    {
                        var defaultValueAttribute = parameterInfo
                            .GetCustomAttribute<DefaultValueAttribute>(inherit: false);

                        if (defaultValueAttribute?.Value == null)
                        {
                            defaultValue = parameterInfo.ParameterType.GetTypeInfo().IsValueType
                                ? Activator.CreateInstance(parameterInfo.ParameterType)
                                : null;
                        }
                        else
                        {
                            defaultValue = defaultValueAttribute.Value;
                        }
                    }

                    _parameterDefaultValues[i] = defaultValue;
                }
            }
        }
    }
}
