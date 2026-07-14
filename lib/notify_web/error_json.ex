defmodule NotifyWeb.ErrorJSON do
  @moduledoc false

  # Renders "404.json", "500.json", etc. into a small JSON body.
  def render(template, _assigns) do
    %{error: %{status: template, message: Phoenix.Controller.status_message_from_template(template)}}
  end
end
