# Esent.ManagedTable
A slightly more verbose implementation than PersistentDictionary, where indexes columns and cursor functions, can be easily written with the same threadsafe structures.

## What is this?

This was originally my playground for learning how to implement Esent, largely by reading through the source code provided for [PersistentDictionary](http://managedesent.codeplex.com/) which has a high performance threadsafe implementation of cursors / Esent sessions, this structure will suit a lot of people, but in my case I needed granular control over the columns, indexes and queries being executed.

To do this with mimimal new code in each instance I adapted a lot of the structures and locking from the PersistentDictionary and using abstracts / generics allow developers to write operates with similar guarantees. 

There is a very simple example cache which uses a lot of the ideas and some structure from the [aspnetcore MemoryCache](https://github.com/aspnet/Caching/tree/dev/src/Microsoft.Extensions.Caching.Memory), such as absolute and sliding expiration, and delegate callbacks.

