import sys

def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)

    if not argv or argv[0] in ("-h", "--help"):
        print(
            "wallaby_mw pipeline\n\n"
            "Usage:\n"
            "  python -m wallaby_mw <command> [args]\n\n"
            "Commands:\n"
            "  casda-download   Download WALLABY MW products from CASDA\n"
            "  apply-subfits    Run subfits on CASDA cubes\n"
        )
        return 0

    cmd = argv.pop(0)

    if cmd in ("casda-download", "casda_download"):
        from wallaby_mw.stages.casda_download import main as casda_main
        return casda_main(argv)

    if cmd in ("apply-subfits", "apply_subfits"):
        from wallaby_mw.stages.apply_subfits import main as subfits_main
        return subfits_main(argv)

    print(f"Unknown command: {cmd}\nUse --help for usage.")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())