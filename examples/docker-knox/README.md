## The _ Parameter in Knox WebHDFS URLs

Purpose: The underscore (_) parameter contains encrypted query parameters that Knox uses to protect internal Hadoop cluster details.

What it encrypts:
- Datanode host and port information ({datanode-host} and {datanode-port})
- Original query parameters from the backend WebHDFS response
- Internal cluster topology details that should not be exposed to external clients

How it works:
1. When a WebHDFS request returns a Location header with a DataNode URL from the backend
2. Knox rewrites it to: https://{gateway-host}:{gateway-port}/{gateway-path}/{cluster-name}/webhdfs/data/v1/{path}?_={encrypted-query-parameters}
3. The {encrypted-query-parameters} contains the sensitive backend information in encrypted form

Security aspects:
- The encryption keys are topology-specific (different for each Hadoop cluster)
- This allows failover between gateway instances while maintaining security
- It's a security feature to protect internal cluster topology from external exposure

The Knox rewrite rules in /Users/martin/Workspaces/scala/delta-sharing/examples/docker-knox/configs/knox/data/services/webhdfs/2.4.0/rewrite.xml:12 contain this pattern:

  <rewrite template="{$frontend[url]}/webhdfs/data/v1/{path=**}?{scheme}?host={$hostmap(host)}?{port}?{op}?{delegation}?{namenoderpcaddress}?{buffersize}"/>
  <encrypt-query/>

The <encrypt-query/> directive is what creates that encrypted _ parameter from all the query parameters listed in the template.
