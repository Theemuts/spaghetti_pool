defmodule SpaghettiPool.FSM do
  @moduledoc false

  defmacro __using__(_) do
    quote do
      @behaviour :gen_fsm

      def start(args, opts) do
        :gen_fsm.start(__MODULE__, args, opts)
      end

      def start(name, args, opts) do
        :gen_fsm.start(name, __MODULE__, args, opts)
      end

      def start_link(args, opts) do
        :gen_fsm.start_link(__MODULE__, args, opts)
      end

      def start_link(name, args, opts) do
        :gen_fsm.start_link(name, __MODULE__, args, opts)
      end

      def init(args) do
        {:ok, :idle, args}
      end

      def handle_event(_, state_name, state_data) do
        {:next_state, state_name, state_data}
      end

      def handle_sync_event(_, _, state_name, state_data) do
        {:next_state, state_name, state_data}
      end

      def handle_info(_, state_name, state_data) do
        {:next_state, state_name, state_data}
      end

      def terminate(_reason, _state_name, _state_data) do
        :ok
      end

      def code_change(_, state_name, state_data, _) do
        {:ok, state_name, state_data}
      end

      defoverridable [start: 2, start_link: 2, start: 3, start_link: 3, init: 1, handle_event: 3, handle_sync_event: 4, handle_info: 3, terminate: 3, code_change: 4]
    end
  end
end


