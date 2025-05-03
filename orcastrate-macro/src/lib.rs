extern crate proc_macro;
use proc_macro::TokenStream;
use quote::{quote, format_ident};
use syn::{parse_macro_input, ItemFn, FnArg, PatType};


#[proc_macro_attribute]
pub fn orca_task(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);

    let func_name = &func.sig.ident;
    let func_vis = &func.vis;
    let func_asyncness = &func.sig.asyncness; // Should be async
    let func_generics = &func.sig.generics; // Keep generics if any
    let return_type = &func.sig.output;
         // Keep return type

    // --- Argument Parsing (All args are task parameters now) ---
    let task_args: Vec<_> = func.sig.inputs.iter().map(|arg| match arg {
        FnArg::Typed(pat_type) => pat_type.clone(),
        _ => panic!("Unsupported argument type (e.g., self) in task function"),
    }).collect();

    let task_arg_names: Vec<_> = task_args.iter().map(|pt| &pt.pat).collect();
    let task_arg_types: Vec<_> = task_args.iter().map(|pt| &pt.ty).collect();

    // --- Generated Code ---
    let task_struct_name = format_ident!("{}", func_name.to_string()); // e.g., `foo` -> `foo` struct
    let task_args_struct_name = format_ident!("{}Args", func_name.to_string()); // e.g., `FooArgs`

    // 1. Argument Struct (for serialization)
    let args_struct_def = quote! {
        #[derive(::serde::Serialize, ::serde::Deserialize, Debug, Clone)]
        struct #task_args_struct_name #func_generics { // Include generics if func has them
            #( #task_arg_names: #task_arg_types ),*
        }
    };

    // 2. Task Handle Struct
    let handle_struct_def = quote! {
        #[derive(Clone)]
        #func_vis struct #task_struct_name #func_generics { // Include generics
            task_name: String,
            worker_ref: ::kameo::prelude::ActorRef<::orcastra::worker::Worker>,
             // Include PhantomData if the struct uses generics from the function
            _marker: ::std::marker::PhantomData<(#(#task_arg_types),*)>,
        }

        // Add where clause if func_generics is not empty
        impl #func_generics #task_struct_name #func_generics {
            pub fn new(
                task_name: String,
                worker_ref: ::kameo::prelude::ActorRef<::orcastra::worker::Worker>
             ) -> Self {
                Self { task_name, worker_ref, _marker: ::std::marker::PhantomData }
            }

            // The `submit` method takes the original function args
            pub async fn submit(&self, #( #task_arg_names: #task_arg_types ),* ) -> Result<(), ::orcastra::worker::WorkerError>
            // Add where clause if func_generics is not empty
            {
                let args = #task_args_struct_name {
                    #( #task_arg_names: #task_arg_names.clone() ),* // Clone args if needed, or require Clone bound
                };
                let serialized_args = ::serde_json::to_string(&args)
                    .map_err(|e| ::orcastra::worker::WorkerError(format!("Args serialization failed: {}", e)))?;

                // Use a modified SubmitTask message
                let message = ::orcastra::messages::SubmitTaskArgs { // NEW MESSAGE TYPE
                    task_name: self.task_name.clone(),
                    args: serialized_args,
                    // TODO: Add options like delay, retries here later
                };

                // Send to worker (might need adjustment if SubmitTaskArgs is handled differently)
                 self.worker_ref.ask(message).await // Use ask if worker needs to confirm registration before submit
                    .map_err(|e| ::orcastra::worker::WorkerError(format!("Kameo ask error: {}", e)))? // Handle Kameo Error
                    .map_err(|e| ::orcastra::worker::WorkerError(format!("Worker submission error: {}", e)))?; // Handle WorkerError from worker's reply
                 Ok(())
            }
        }
    };

    // 3. The Original Function Definition (remains unchanged)
    let original_func_def = &func;

    // 4. Task Runner Logic (Simplified - conceptual)
    // This logic needs to live somewhere. Could be a generated impl or part of a new system.
    // We'll generate a function that encapsulates creating the future.
    let future_creator_fn_name = format_ident!("create_{}_future", func_name);

    let future_creator_fn = quote! {
         // This function takes serialized args and returns a future that executes the task
         fn #future_creator_fn_name #func_generics (serialized_args: String) // Add where clause if needed
             -> Result<::std::pin::Pin<Box<dyn ::std::future::Future<Output = #return_type> + Send>>, ::orcastra::task::OrcaError>
         // Add where clause for serde bounds etc.
         where
             // Add Send + 'static bounds etc. to generic types if necessary
         {
             let args: #task_args_struct_name #func_generics = ::serde_json::from_str(&serialized_args)
                 .map_err(|e| ::orcastra::task::OrcaError(format!("Args deserialization failed: {}", e)))?;

             // Create the future that calls the original function
             let task_future = Box::pin(async move {
                 // Directly call the original function with deserialized args
                 #func_name(#( args.#task_arg_names ),*).await
             });
             Ok(task_future)
         }
    };


    // 5. Registration Function (Example)
    let registration_fn_name = format_ident!("register_{}", func_name.to_string());
    let registration_fn = quote! {
         #func_vis fn #registration_fn_name #func_generics ( // Add where clause
             worker_ref: ::kameo::prelude::ActorRef<::orcastra::worker::Worker>
         ) -> #task_struct_name #func_generics {
             let task_name = stringify!(#func_name).to_string();

             // TODO: Tell the worker how to create/run this task.
             // This needs a new message, e.g., RegisterTaskRunner,
             // passing the task_name and a way to invoke future_creator_fn_name.
             // For now, just create the handle.
             // worker_ref.tell(RegisterTaskDefinition { name: task_name.clone(), future_creator: #future_creator_fn_name }).await;

             #task_struct_name::new(task_name, worker_ref)
         }
    };

    // Combine generated code
    let expanded = quote! {
        #args_struct_def
        #handle_struct_def
        #original_func_def // Keep the original function
        #future_creator_fn // Generated function to create the future
        #registration_fn // Generated function to get the handle
    };

    TokenStream::from(expanded)
}