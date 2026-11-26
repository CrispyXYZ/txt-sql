import argparse

from .cli import run_cli


def main() -> None:
    parser = argparse.ArgumentParser(description='TxtSql CLI / GUI entry point')
    parser.add_argument('--gui', action='store_true', help='Launch the Tkinter graphical interface')
    parser.add_argument('--dir', default='.', help='Database working directory (default: current directory)')
    args = parser.parse_args()

    if args.gui:
        from .gui import run_gui
        run_gui(data_dir=args.dir)
    else:
        run_cli(data_dir=args.dir)


if __name__ == '__main__':
    main()
