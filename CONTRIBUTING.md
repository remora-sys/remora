# Contributing to Remora

Thank you for your interest in contributing to Remora!

## Adding Contributors

This project uses [all-contributors](https://allcontributors.org/) to recognize contributors.

### Using the all-contributors CLI

1. Install the all-contributors CLI:
   ```bash
   npm install -g all-contributors-cli
   ```

2. Add a contributor:
   ```bash
   all-contributors add <username> <contribution-type>
   ```

   Example:
   ```bash
   all-contributors add johndoe code,doc
   ```

   Contribution types include: `code`, `doc`, `test`, `bug`, `design`, `review`, etc.
   See the full list at: https://allcontributors.org/docs/en/emoji-key

3. The CLI will automatically update both `.all-contributorsrc` and `README.md`.

### Manual Addition

You can also manually edit `.all-contributorsrc` to add a contributor:

```json
{
  "login": "username",
  "name": "Full Name",
  "avatar_url": "https://avatars.githubusercontent.com/u/USER_ID",
  "profile": "https://github.com/username",
  "contributions": [
    "code",
    "doc"
  ]
}
```

Then run:
```bash
all-contributors generate
```

### Using Comments (Recommended)

You can also add contributors by commenting on issues or pull requests:

```
@all-contributors please add @username for code and documentation
```

The bot will automatically create a PR to add the contributor.

## Development

See the main [README.md](README.md) for instructions on building and testing the project.
