#!/usr/bin/env bash
set -euo pipefail

die() {
  printf 'parable install: %s\n' "$*" >&2
  exit 1
}

need() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is required but was not found on PATH"
}

script_dir="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
runtime_dir="$script_dir/runtime"
version_file="$runtime_dir/VERSION"

[[ -f "$version_file" ]] || die "the installed skill is incomplete (runtime/VERSION is missing)"
version="$(tr -d '[:space:]' < "$version_file")"
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "the bundled runtime version is invalid"

for required in \
  "$runtime_dir/bin/parable.js" \
  "$runtime_dir/lib/onboarding.js" \
  "$runtime_dir/patches/cliproxyapi-v7.2.88-claude-effort.patch" \
  "$script_dir/scripts/parable.py"; do
  [[ -f "$required" && ! -L "$required" ]] || die "the installed skill is incomplete (${required#"$script_dir/"} is missing)"
done

need node
need python3
need cp
need diff
need mktemp

install_root="${PARABLE_INSTALL_ROOT:-$HOME/.local/share/parable}"
bin_dir="${PARABLE_BIN_DIR:-$HOME/.local/bin}"
destination="$install_root/$version"
command_path="$bin_dir/parable"

umask 077
if [[ -e "$install_root" && ( ! -d "$install_root" || -L "$install_root" ) ]]; then
  die "$install_root exists but is not a safe directory"
fi
if [[ -e "$bin_dir" && ( ! -d "$bin_dir" || -L "$bin_dir" ) ]]; then
  die "$bin_dir exists but is not a safe directory"
fi
mkdir -p -- "$install_root" "$bin_dir"

stage="$(mktemp -d "$install_root/.parable-$version.XXXXXX")"
link_stage=""
cleanup() {
  rm -rf -- "$stage"
  [[ -z "$link_stage" ]] || rm -f -- "$link_stage"
}
trap cleanup EXIT

mkdir -p -- "$stage/bin" "$stage/lib" "$stage/patches" "$stage/skills"
cp -- "$runtime_dir/bin/parable.js" "$stage/bin/parable.js"
cp -- "$runtime_dir/lib/onboarding.js" "$stage/lib/onboarding.js"
cp -- "$runtime_dir/patches/cliproxyapi-v7.2.88-claude-effort.patch" "$stage/patches/"
cp -R -- "$script_dir" "$stage/skills/parable"
find "$stage" -type d -exec chmod 700 {} +
find "$stage" -type f -exec chmod 600 {} +
chmod 700 "$stage/bin/parable.js" "$stage/skills/parable/parable.sh"
find "$stage/skills/parable/scripts" -type f \( -name '*.sh' -o -name '*.py' \) -exec chmod 700 {} +

if [[ -e "$destination" ]]; then
  [[ -d "$destination" && ! -L "$destination" ]] || die "$destination exists but is not a safe directory"
  if ! diff -qr "$stage" "$destination" >/dev/null; then
    die "$destination differs from the bundled $version runtime; refusing to overwrite it"
  fi
  rm -rf -- "$stage"
  stage=""
  printf 'parable runtime: already installed (%s)\n' "$version"
else
  mv -- "$stage" "$destination"
  stage=""
  printf 'parable runtime: installed %s -> %s\n' "$version" "$destination"
fi

if [[ -e "$command_path" || -L "$command_path" ]]; then
  if [[ ! -L "$command_path" ]]; then
    die "$command_path already exists and is not managed by Parable"
  fi
  existing_target="$(readlink "$command_path")"
  case "$existing_target" in
    "$install_root"/*/bin/parable.js) ;;
    *) die "$command_path is an unrelated symlink; refusing to overwrite it" ;;
  esac
fi

link_stage="$bin_dir/.parable-link.$$"
ln -s -- "$destination/bin/parable.js" "$link_stage"
mv -f -- "$link_stage" "$command_path"
link_stage=""
printf 'parable command: %s\n' "$command_path"

case ":$PATH:" in
  *":$bin_dir:"*) ;;
  *)
    if [[ "$bin_dir" != "$HOME/.local/bin" ]]; then
      die "$bin_dir is not on PATH; add it before using PARABLE_BIN_DIR"
    fi
    shell_name="$(basename -- "${SHELL:-sh}")"
    case "$shell_name" in
      bash) profile="$HOME/.bashrc" ;;
      zsh) profile="$HOME/.zshrc" ;;
      *) profile="$HOME/.profile" ;;
    esac
    marker="# Added by Parable: user commands"
    if [[ ! -f "$profile" ]] || ! grep -Fq "$marker" "$profile"; then
      profile_existed=false
      [[ -e "$profile" ]] && profile_existed=true
      {
        printf '\n%s\n' "$marker"
        printf 'case ":$PATH:" in *":$HOME/.local/bin:"*) ;; *) export PATH="$HOME/.local/bin:$PATH" ;; esac\n'
      } >> "$profile"
      if [[ "$profile_existed" == false ]]; then
        chmod 600 "$profile"
      fi
      printf 'parable PATH: added $HOME/.local/bin to %s\n' "$profile"
    fi
    ;;
esac

no_auth=false
for argument in "$@"; do
  [[ "$argument" == "--no-auth" ]] && no_auth=true
done

"$command_path" setup "$@"

if [[ "$no_auth" == true ]]; then
  printf '\nSetup is staged but subscriptions are not authorized.\n'
  printf 'Authorize every selected missing provider in one interactive flow.\n'
  printf 'Open a new terminal and run:\n\n'
  printf '  parable auth login\n\n'
  printf 'Keep that command running until authorization completes; it prints the final handoff.\n'
  exit 0
fi

printf '\nIn a new terminal, open your project and run:\n\n'
printf '  parable\n'
