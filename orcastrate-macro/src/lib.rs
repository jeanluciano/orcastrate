extern crate proc_macro;
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{FnArg, ItemFn, ReturnType, parse_macro_input};

#[proc_macro_attribute]
pub fn orca_task(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);

    // --- Check for Generics (Not Supported) ---
    if !func.sig.generics.params.is_empty() {
        panic!("Generic task functions are not supported by #[orca_task]");
    }

    let func_name = &func.sig.ident;
    let func_vis = &func.vis;

    let return_type = &func.sig.output;
    let (ok_type, err_type) = match return_type {
        ReturnType::Type(_, ty) => {
            if let syn::Type::Path(type_path) = &**ty {
                if let Some(segment) = type_path.path.segments.last() {
                    if segment.ident == "Result" {
                        if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                            if args.args.len() == 2 {
                                let ok = &args.args[0];
                                let err = &args.args[1];
                                (quote! { #ok }, quote! { #err })
                            } else {
                                panic!("Result must have two type arguments");
                            }
                        } else {
                            panic!("Result must have angle bracketed arguments");
                        }
                    } else {
                        panic!("Task function must return a Result");
                    }
                } else {
                    panic!("Unsupported return type path");
                }
            } else {
                panic!("Task function must return a Result type");
            }
        }
        ReturnType::Default => panic!("Task function must return a Result"),
    };

    // --- Argument Parsing ---
    let task_args: Vec<_> = func
        .sig
        .inputs
        .iter()
        .map(|arg| match arg {
            FnArg::Typed(pat_type) => pat_type.clone(),
            _ => panic!("Unsupported argument type (e.g., self) in task function"),
        })
        .collect();

    let task_arg_names: Vec<_> = task_args.iter().map(|pt| &pt.pat).collect();
    let task_arg_types: Vec<_> = task_args.iter().map(|pt| &pt.ty).collect();

    // --- Generated Names ---
    let task_struct_name = format_ident!("{}", func_name.to_string()); // e.g., `my_async_task` struct
    let task_args_struct_name = format_ident!("{}Args", func_name.to_string()); // e.g., `MyAsyncTaskArgs`
    let future_creator_fn_name = format_ident!("create_{}_future", func_name);
    let task_name_literal = func_name.to_string();

    // 1. Argument Struct (for serialization)
    let args_struct_def = quote! {
        #[derive(::serde::Serialize, ::serde::Deserialize, Debug, Clone)]
        #func_vis struct #task_args_struct_name {
            #( pub #task_arg_names: #task_arg_types ),* // Make fields public for direct access
        }
    };

    let handle_struct_def = quote! {
            #[derive(Debug)]
            #func_vis struct #task_struct_name {
                pub task_name: String,
                pub worker_ref: ::kameo::prelude::ActorRef<::orcastrate::worker::Worker>,
                pub serialized_args: Option<String>,
                pub error: Option<String>,
                pub delay_by: Option<i64>,
            }

            impl #task_struct_name {
                 fn register(
                    worker_ref: ::kameo::prelude::ActorRef<::orcastrate::worker::Worker>
                 ) -> Self {
                    Self { task_name: #task_name_literal.to_string(), worker_ref, serialized_args: None, error: None, delay_by: None }
                }
                fn get_worker_id(&self) -> String {
                    self.worker_ref.id().to_string()
                }

                pub fn submit(mut self, #( #task_arg_names: #task_arg_types ),* ) -> Self
                {
                    let args_struct_instance = #task_args_struct_name {
                        #( #task_arg_names: #task_arg_names.clone() ),*
                    };

                    let serialized_args_result = ::serde_json::to_string(&args_struct_instance)
                        .map_err(|e| ::orcastrate::error::OrcaError(format!("Args serialization failed for task '{}': {}", self.task_name, e)));


                    if let Ok(serialized_args_string) = serialized_args_result {
                        self.serialized_args = Some(serialized_args_string);
                    } else {
                        // Properly return the serialization error
                        self.error = Some(serialized_args_result.unwrap_err().to_string());
                    }
                    self
                }

                pub fn start(mut self, delay: Option<i64>) -> Self{
                    self.delay_by = delay;
                    self
                }
            }
            impl ::std::future::IntoFuture for #task_struct_name {
                type Output = Result<::orcastrate::seer::Handler, ::orcastrate::error::OrcaError>;
                type IntoFuture = ::std::pin::Pin<Box<dyn ::std::future::Future<Output = Self::Output> + Send>>;

                fn into_future(self) -> Self::IntoFuture {
                  Box::pin(async move {
                    if let Some(err_msg) = self.error {
                        return Err(::orcastrate::error::OrcaError(err_msg));
                    }
                    
                    let message = ::orcastrate::messages::StartRun {
                        task_name: self.task_name.clone(),
                        args: self.serialized_args.clone(),
                        delay: self.delay_by,
                    };
                    let seer_ref = self.worker_ref.ask(message).await;
                    match seer_ref {
                        Ok(seer_ref) => Ok(seer_ref),
                        Err(e) => Err(::orcastrate::error::OrcaError(format!("Error starting task: {}", e))),
                    }
                  })
                }
            }
        };
    

        

    // 3. The Original Function Definition (remains unchanged)
    let original_func_def = &func;

    // 4. Task Future Creator Function (non-generic signature)
    let future_creator_fn = quote! {
         fn #future_creator_fn_name (serialized_args: String)
             -> Result<::orcastrate::task::TaskFuture, ::orcastrate::error::OrcaError>
         {
             let args: #task_args_struct_name = ::serde_json::from_str(&serialized_args)
                 .map_err(|e| ::orcastrate::error::OrcaError(format!("Args deserialization failed for '{}': {}", #task_name_literal, e)))?;

             // Create the future that calls the original function
             let task_future = Box::pin(async move {
                 // Function call - NO GENERICS
                 let result: Result<#ok_type, #err_type> = #func_name(#( args.#task_arg_names ),*).await;
                 // Serialize the result or error to String
                 match result {
                    Ok(ok_val) => ::serde_json::to_string(&ok_val)
                        .map_err(|e| ::orcastrate::error::OrcaError(format!("Result serialization failed: {}", e))),
                    Err(err_val) => Err(::orcastrate::error::OrcaError(
                        ::serde_json::to_string(&err_val)
                            .unwrap_or_else(|e| format!("Error serialization failed: {}", e))
                    )),
                 }
             });
             // Wrap the Box<Pin<...>> future in the outer Result required by TaskRunnerFn
             let wrapped_future: ::orcastrate::task::TaskFuture = Box::pin(async move {
                match task_future.await {
                    Ok(s) => Ok(s),
                    Err(orca_err) => Err(orca_err.to_string()),
                }
             });

             Ok(wrapped_future)
         }
    };

    // 5. Static Registration using Inventory
    let static_registration = quote! {
        ::inventory::submit! {
            ::orcastrate::task::StaticTaskDefinition {
                task_name: #task_name_literal,
                task_future: #future_creator_fn_name,
            }
        }
    };

    let expanded = quote! {
        #args_struct_def
        #handle_struct_def
        #original_func_def
        #future_creator_fn
        #static_registration
    };

    TokenStream::from(expanded)
}
