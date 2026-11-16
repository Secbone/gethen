# @gethen/dsh-client

DSH client-shell settings page for the Gethen overview.

The host half is empty. The browser half registers `settings.section` with id
`gethen` and reads `/gethen/overview` from the `@gethen/dsh/panel` host route.
The page is intentionally dependency-free (React from the shell platform) and
shows person-blocked Tickets, open work and decision counts.

Mount the host/panel plugins and this client plugin in the same web profile.
The client module system serves `./client` only when the Loader row uses the
bare package name.
