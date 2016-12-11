[![Build Status](https://travis-ci.org/zezutom/NiFiByExample.svg?branch=master)](https://travis-ci.org/zezutom/NiFiByExample)
[![Coverage Status](https://coveralls.io/repos/github/zezutom/NiFiByExample/badge.svg?branch=master)](https://coveralls.io/github/zezutom/NiFiByExample?branch=master)

# NiFi by Example

## Custom Processors
|  Description | Source Code |
| ------------- | ------------- |
| HTTP POST with JSON body | [PostHTTPWithJsonBody.java](nifi-processors/src/main/java/org/zezutom/processors/nifi/example/PostHTTPWithJsonBody.java) |

## NiFi Templates
### HTTP Post
|  Description | Template |
| ------------- | ------------- |
| Post JSON using the InvokeHTTP processor.  | [POST_via_InvokeHTTP.xml](templates/http_post/POST_via_InvokeHTTP.xml)  |
| Post JSON using the PostHTTP processor.  | [POST_via_PostHTTP.xml](templates/http_post/POST_via_PostHTTP.xml)  |
| Post a dynamically built JSON.  | [POST_with_a_Dynamic_Body.xml](templates/http_post/POST_with_a_Dynamic_Body.xml)  |
| Post a dynamically built JSON using regex text replacement.  | [POST_with_a_Dynamic_Body_using_ReplaceText.xml](templates/http_post/POST_with_a_Dynamic_Body_using_ReplaceText.xml)  |
| Post JSON using a custom processor [PostHTTPWithJsonBody](nifi-processors/src/main/java/org/zezutom/processors/nifi/example/PostHTTPWithJsonBody.java).  | [POST_with_a_Dynamic_Body_using_a_Custom_Processor.xml](templates/http_post/POST_with_a_Dynamic_Body_using_a_Custom_Processor.xml)  |
| Download all examples as a single package.  | [HTTP_POST_Examples.xml](templates/http_post/HTTP_POST_Examples.xml)  |

### SSL
| Description  | Template |
| ------------- | ------------- |
| SSLContextService config (key store, trust store)  | [SSL_Service_Config.xml](templates/http_post/SSL_Service_Config.xml)  |

### Streaming
| Description  | Template |
| ------------- | ------------- |
| Web Proxy Analysis: [Proxy Log Generator](https://github.com/zezutom/proxy-log-generator) + Kafka  | [Web_Proxy_Analysis.xml](templates/streaming/web_proxy_analysis.xml.xml)  |
