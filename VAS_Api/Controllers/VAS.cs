using Microsoft.AspNetCore.Mvc;
using VAS_Api.Models;
using StackExchange.Redis;

namespace VAS_Api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class VAS : ControllerBase
    {
        private readonly string _connectionString;
        private readonly IConfiguration _configuration;
        private ConnectionMultiplexer redisConnection;
        private IDatabase redisDatabase;
        private string redisHostIp;
        private string redisPort;
        private string redisPassword;
        
        public VAS(IConfiguration configuration)
        {
            
            _configuration = configuration;
            redisHostIp = _configuration["RedisSettings:RedisHostIp"];
            redisPort = _configuration["RedisSettings:RedisPort"];
            redisPassword = _configuration["RedisSettings:RedisPassword"];

            try
            {
                var configurationOptions = new ConfigurationOptions
                {
                    EndPoints = { $"{redisHostIp}:{redisPort}" },
                    Password = redisPassword
                };
                redisConnection = ConnectionMultiplexer.Connect(configurationOptions);
                redisDatabase = redisConnection.GetDatabase();
            }
            catch (Exception Ex)
            {
                var Response = new Response
                {
                    Statuscode = 500,
                    Status = "Fail",
                    Message = "Error : " + Ex.Message
                };
            }
        }

        [HttpGet("GetAllDataFromRedisByKey")]
        public async Task<IActionResult> GetAllDataFromRedisByKey([FromQuery] string redisKey)
        {
            if (string.IsNullOrEmpty(redisKey))
            {
                return BadRequest(new Response
                {
                    Statuscode = 400,
                    Status = "Fail",
                    Message = "Redis key cannot be null or empty."
                });
            }

            try
            {
                var hashEntries = await redisDatabase.HashGetAllAsync(redisKey);  
                
                if (hashEntries.Length == 0)
                {
                    return NotFound(new Response
                    {
                        Statuscode = 404,
                        Status = "Fail",
                        Message = "No data found for the provided redis key."
                    });
                }

                var data = hashEntries.ToDictionary(
                    entry => entry.Name.ToString(),
                    entry => entry.Value.ToString()
                );

                return Ok(data);
            }
            catch (Exception Ex)
            {
                var Response = new Response
                {
                    Statuscode = 500,
                    Status = "Fail",
                    Message = "Error : " + Ex.Message
                };

                return Ok(Response);
            }
        }

        [HttpGet("DeleteFromRedisByKey")]
        public async Task<IActionResult> DeleteFromRedisByKey([FromQuery] string redisKey)
        {
            if (string.IsNullOrEmpty(redisKey))
            {
                return BadRequest(new Response
                {
                    Statuscode = 400,
                    Status = "Fail",
                    Message = "Redis key cannot be null or empty."
                });
            }

            try
            {
                 var hashEntries = redisDatabase.HashGetAll(redisKey);

                if (hashEntries.Length == 0)
                {
                    return NotFound(new Response
                    {
                        Statuscode = 404,
                        Status = "Fail",
                        Message = "No data found for the provided redis key."
                    });
                }
                else
                {
                    bool deleted = await redisDatabase.KeyDeleteAsync(redisKey);
                    var Response = new Response
                    {
                        Statuscode = 200,
                        Status = "Success",
                        Message = "Data deleted successfully",
                    };
                    return Ok(Response);
                }                
            }
            catch (Exception Ex)
            {
                var Response = new Response
                {
                    Statuscode = 500,
                    Status = "Fail",
                    Message = "Error : " + Ex.Message
                };

                return Ok(Response);
            }
        }

        [HttpGet("GetCountAndColorDicFromRedis")]
        public async Task<IActionResult> GetCountAndColorDicFromRedis([FromQuery] string redisKeyCount, [FromQuery] string redisKeyColor)
        {
            if (string.IsNullOrEmpty(redisKeyCount))
            {
                return BadRequest(new Response
                {
                    Statuscode = 400,
                    Status = "Fail",
                    Message = "Redis key cannot be null or empty."
                });
            }
            try
            {
                var hashEntries = await redisDatabase.HashGetAllAsync(redisKeyCount);
                if (hashEntries.Length == 0)
                {
                    return NotFound(new Response
                    {
                        Statuscode = 404,
                        Status = "Fail",
                        Message = "No data found for the provided Redis key."
                    });
                }
               
                var data = hashEntries.ToDictionary(
                    entry => entry.Name.ToString(),
                    entry => entry.Value.ToString()
                );

                try
                {
                    var hashEntries1 = redisDatabase.HashGetAll(redisKeyCount);
                    var dictionary1 = hashEntries.ToDictionary(
                        entry => entry.Name.ToString(),
                        entry => entry.Value.ToString()
                    );
                    
                    var hashEntries2 = redisDatabase.HashGetAll(redisKeyColor);
                    var dictionary2 = hashEntries2.ToDictionary(
                        entry => entry.Name.ToString(),
                        entry => entry.Value.ToString()
                    );

                    var combinedData = new Dictionary<string, string>();
                    foreach (var key in dictionary1.Keys)
                    {
                        if (dictionary2.TryGetValue(key, out string color))
                        {
                            var colorValue = $"{key}: {color}";
                            combinedData[key] = $"{dictionary1[key]},  -- {colorValue}";
                        }
                    }

                    var response = new
                    {
                        Header = $"{redisKeyCount} And {redisKeyColor}",
                        Data = combinedData
                    };
                    return Ok(response);
                }
                catch (Exception ex)
                {
                    return StatusCode(500, new { Message = $"Error: {ex.Message}" });
                }
            }
            catch (Exception Ex)
            {
                var Response = new Response
                {
                    Statuscode = 500,
                    Status = "Fail",
                    Message = "Error : " + Ex.Message
                };
                return Ok(Response);
            }
        }
        
        [HttpGet("GetAllHashKeyFromRedis")]
        public async Task<IActionResult> GetAllHashKeyFromRedis()
        {
            try
            {
                var configurationOptions = new ConfigurationOptions
                {
                    EndPoints = { $"{redisHostIp}:{redisPort}" },
                    Password = redisPassword
                };

                var connection = await ConnectionMultiplexer.ConnectAsync(configurationOptions);
                var server = connection.GetServer(configurationOptions.EndPoints.First());
                var db = connection.GetDatabase();

                var hashKeys = new Dictionary<RedisKey, List<string>>();
                var cursor = 0L;
                var batchSize = 1000;

                do
                {
                    var result = await db.ExecuteAsync("SCAN", cursor.ToString(), "MATCH", "*", "COUNT", batchSize);
                    var resultArray = (RedisResult[])result;

                    cursor = (long)resultArray[0];
                    var keys = (RedisKey[])resultArray[1];

                    var tasks = keys.Select(async key =>
                    {
                        if (await db.KeyTypeAsync(key) == RedisType.Hash)
                        {
                            var hashFields = await db.HashKeysAsync(key);
                            lock (hashKeys) 
                            {
                                hashKeys[key] = hashFields.Select(f => f.ToString()).ToList();
                            }
                        }
                    });
                    await Task.WhenAll(tasks);
                } while (cursor != 0);

                var returnValue = hashKeys.Keys.Select(key => key.ToString()).ToList();
                return Ok(returnValue);
            }
            catch (Exception Ex)
            {
                var Response = new Response
                {
                    Statuscode = 500,
                    Status = "Fail",
                    Message = "Error : " + Ex.Message
                };
                return Ok(Response);
            }
        }
             
    
    }
}
