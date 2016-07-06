%%--------------------------------------------------------------------
%% Copyright (c) 2015-2016 J Phani Mahesh <opensource@phanimahesh.me>
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc emqttd pgsql plugin.
-module(emqttd_ratelimit_pgsql).

-include("../../../include/emqttd.hrl").

-export([set_rl/3]).



set_rl(_ConnAck,
       Client = #mqtt_client{client_pid = Pid, client_id = ClientId, username = Username},
       {SuperQuery, {RlSql, RlParams}, DefaultRl}) ->
    Rl = case emqttd_plugin_pgsql:is_superuser(SuperQuery, Client) of
        false -> case emqttd_plugin_pgsql:equery(RlSql, RlParams, Client) of
                    {ok, _, []} ->
                        DefaultRl;
                    {ok, _, Rows} ->
                        get_rl_from_results(ClientId, Username, Rows, DefaultRl);
                    {error, Error} ->
                        %% Can't do much. Log and apply default limits
                        %% Or should we reject connection?
                        DefaultRl
                 end;
        true  -> ok
    end,
    case Rl of
      ignore -> ignore;
      {Burst, Rate} ->
        Rl1 =esockd_ratelimit:new(Burst, Rate),
        emqttd_client:set_rate_limit(Pid, Rl1)
    end,
    {ok, Client}.

get_rl_from_results(ClientId, Username, Rows, DefaultRl) ->
   Rls = [
          get_rl_clientid(ClientId, Rows),
          get_rl_username(Username, Rows),
          get_rl_all(Rows)
          | DefaultRl],
   hd(lists:dropwhile(fun(X) -> X =:= undefined end, Rls)).

get_rl_clientid(ClientId, [{_, ClientId, Burst, Rate}|_]) -> {Burst, Rate};
get_rl_clientid(ClientId, [_|T]) -> get_rl_clientid(ClientId,T);
get_rl_clientid(_, []) -> undefined.

get_rl_username(Username, [{Username, _, Burst, Rate}|_]) -> {Burst, Rate};
get_rl_username(Username, [_|T]) -> get_rl_username(Username,T);
get_rl_username(_, []) -> undefined.

get_rl_all([{<<"$all">>, _, Burst, Rate}|_]) -> {Burst, Rate};
get_rl_all([{_, <<"$all">>, Burst, Rate}|_]) -> {Burst, Rate};
get_rl_all([_|T]) -> get_rl_all(T);
get_rl_all([]) -> undefined.

