<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

# Glider

Glider is an ephemeral storage solution with capacity for ephemeral computation.
It is specially designed to address the data-shipping problem in serverless analytics.
Glider integrates serverless ephemeral stateful near-data computation within a storage service specifically designed to collaborate with existing FaaS platforms.
It is tailored for handling intermediate data, which is a well-known problem of running data analytics workloads on serverless services, which are ephemeral and stateless, cannot communicate directly, and have limited resources.
The main objective is to minimize the amount of data transferred between compute and storage systems, thereby optimizing and enhancing the connections between various stages of serverless functions.
To achieve this, Glider introduces **storage actions**, which are named storage elements within the storage namespace.
They do not only encapsulate stateful computation but also provide I/O streams to efficiently handle large volumes of data.

Glider is implemented by extending [Apache Crail](https://github.com/apache/incubator-crail), a fast multi-tiered distributed storage system designed from ground up for high-performance network and storage hardware.
As such it is easily managed in a similar way.
Glider adds a new storage server type ('org.apache.crail.storage.active.ActiveStorageTier') and a new storage node type (Actions).

A full description is published in the following research paper:

Daniel Barcelona-Pons, Pedro García-López and Bernard Metzler. 2023. *Glider: Serverless Ephemeral Stateful Near-data Computation*. 


