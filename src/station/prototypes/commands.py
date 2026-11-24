from station.prototypes.typing_indicator import load_typing_enabled, save_typing_enabled

BASIC_COMMANDS = {"/help", "/start", "/restart", "/reset", "/stop", "/typing"}


class PrototypeCommands:
    def _kind_help_commands(self):
        return []

    def _settings_extra_lines(self):
        return []

    def _has_model_config(self):
        return hasattr(self, "_config_status_text")

    def _help_commands(self):
        items = [
            ("/help", "Show commands"),
            ("/start", "Start"),
            ("/restart", "Clear this chat and start fresh"),
            ("/stop", "Force-stop the model for this chat"),
            ("/typing", "Show typing while the model works"),
        ]
        if self._has_model_config():
            items.extend(
                [
                    ("/settings", "Show model settings"),
                    ("/key", "Set API key"),
                    ("/model", "Set model"),
                    ("/provider", "Set provider"),
                    ("/base_url", "Set base URL"),
                    ("/config reset", "Clear saved overlay"),
                ]
            )
        items.extend(self._kind_help_commands())
        return items

    def _station_slash_commands(self):
        names = set(BASIC_COMMANDS)
        if self._has_model_config():
            names.update({"/key", "/model", "/provider", "/base_url", "/config", "/settings"})
        for cmd, _desc in self._kind_help_commands():
            token = (cmd or "").split(None, 1)[0].split("@", 1)[0].lower()
            if token:
                names.add(token)
        return names

    def _owns_station_command(self, cmd):
        return (cmd or "").lower() in self._station_slash_commands()

    def _help_text(self):
        seen = set()
        lines = ["Commands:"]
        for cmd, desc in self._help_commands():
            if cmd in seen:
                continue
            seen.add(cmd)
            lines.append("%s - %s" % (cmd, desc))
        return "\n".join(lines)

    async def _send_help(self, *, chat_id, acct_id, reply_to="", platform="", chat_type=""):
        await self.send_outbound(
            text=self._help_text(),
            chat_id=chat_id,
            acct_id=acct_id,
            reply_to=reply_to,
            platform=platform,
            chat_type=chat_type)

    async def _stop_chat(self, *, chat_id, acct_id):
        return

    async def _handle_basic_command(
        self,
        *,
        cmd,
        args,
        chat_id,
        acct_id,
        reply_to,
        model_settings,
        platform="",
        chat_type="",
    ):
        if cmd not in BASIC_COMMANDS:
            return False
        if cmd == "/start":
            await self.send_outbound(
                text="hi",
                chat_id=chat_id,
                acct_id=acct_id,
                reply_to=reply_to,
                platform=platform,
                chat_type=chat_type)
            return True
        if cmd == "/stop":
            await self._stop_chat(chat_id=chat_id, acct_id=acct_id)
            await self.send_outbound(
                text="Stopped.",
                chat_id=chat_id,
                acct_id=acct_id,
                platform=platform,
                chat_type=chat_type)
            return True
        if cmd == "/typing":
            await self._handle_typing_command(
                args=args,
                chat_id=chat_id,
                acct_id=acct_id,
                reply_to=reply_to,
                platform=platform,
                chat_type=chat_type,
            )
            return True
        if cmd in {"/restart", "/reset"}:
            await self._stop_chat(chat_id=chat_id, acct_id=acct_id)
            await self._restart_chat(chat_id=chat_id, acct_id=acct_id)
            await self.send_outbound(
                text="Chat restarted." if cmd == "/restart" else "reset completed",
                chat_id=chat_id,
                acct_id=acct_id,
                platform=platform,
                chat_type=chat_type)
            return True
        await self._send_help(
            chat_id=chat_id,
            acct_id=acct_id,
            reply_to=reply_to,
            platform=platform,
            chat_type=chat_type)
        return True

    def _typing_status_text(self):
        if load_typing_enabled(self.storage_dir):
            return "Typing indicator on.\nUsage: /typing on|off"
        return "Typing indicator off.\nUsage: /typing on|off"

    async def _handle_typing_command(self, *, args, chat_id, acct_id, reply_to, platform="", chat_type=""):
        arg = (args or "").strip().lower()
        if arg == "":
            text = self._typing_status_text()
        elif arg == "on":
            save_typing_enabled(self.storage_dir, True)
            text = "Typing indicator on. The model will show typing while it works."
        elif arg == "off":
            save_typing_enabled(self.storage_dir, False)
            text = "Typing indicator off."
        else:
            text = "Usage: /typing on|off"
        await self.send_outbound(
            text=text,
            chat_id=chat_id,
            acct_id=acct_id,
            reply_to=reply_to,
            platform=platform,
            chat_type=chat_type)
