// CH.Native Hosting sample — end-to-end ASP.NET demonstration of authentication
// AND dependency injection, run against the docker overlay in ./docker.
// Replaces the former CH.Native.Samples.Authentication and
// CH.Native.Samples.DependencyInjection projects in a single web app so the
// keyed mtls / ssh DataSources can actually handshake against the local server.
//
// Layout:
//   Program.cs                          — bootstrap, mounts the three concern groups
//   ServiceRegistration.cs              — every AddClickHouse(...) + health checks
//   AuthEndpoints.cs                    — coordinator that mounts the four /auth/* mappers
//     AuthUserPassword.cs               —   GET /auth/password
//     AuthJwt.cs                        —   GET /auth/jwt
//     AuthSshKey.cs                     —   GET /auth/ssh
//     AuthClientCertificate.cs          —   GET /auth/cert
//     AuthProbe.cs                      —   shared role-probe + RBAC handler
//   DataEndpoints.cs                    — /events/*, /replica/server, /diag/pool, /ping/*
//   Providers/Demo*.cs                  — IClickHouse{Jwt,Certificate,SshKey}Provider stubs
//   Providers/DockerArtifacts.cs        — resolves docker/generated/* paths
//   EventRow.cs                         — POCO for the bulk-insert endpoint
//
// One-time setup:
//   cd docker && ./setup.sh && docker compose up -d && cd ..
//   dotnet run --project samples/CH.Native.Samples.Hosting

using CH.Native.Samples.Hosting;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddClickHouseHostingSample(builder.Configuration);

var app = builder.Build();

app.MapHostingSampleHealthChecks();
app.MapAuthEndpoints();
app.MapDataEndpoints();

app.Run();
