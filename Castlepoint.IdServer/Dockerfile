#FROM microsoft/dotnet:2.1-aspnetcore-runtime AS base
#WORKDIR /app
#EXPOSE 80
#
#FROM microsoft/dotnet:2.1-sdk AS build
#WORKDIR /src
#COPY Castlepoint.IdServer/Castlepoint.IdServer.csproj Castlepoint.IdServer/
#RUN dotnet restore Castlepoint.IdServer/Castlepoint.IdServer.csproj
#COPY . .
#WORKDIR /src/Castlepoint.IdServer
#RUN dotnet build -c Release -r linux-x64 -o /app
#
#FROM build AS publish
#RUN dotnet publish -c Release -r linux-x64 -o /app
#
#FROM base AS final
#WORKDIR /app
#COPY --from=publish /app .
#ENTRYPOINT ["dotnet", "Castlepoint.IdServer.dll"]


FROM microsoft/dotnet:2.1-sdk AS build-env
WORKDIR /app
EXPOSE 443

# Copy csproj and restore as distinct layers
COPY . ./
RUN dotnet restore

# Publish API project
RUN dotnet publish *.csproj -c Release -o out

# Build runtime image
FROM microsoft/dotnet:2.1-aspnetcore-runtime
WORKDIR /app
COPY --from=build-env /app/out .
ENTRYPOINT ["dotnet", "castlepoint.idserver.dll"]
