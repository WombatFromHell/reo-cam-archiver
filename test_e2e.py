"""
End-to-end tests for the Camera Archiver system.
"""

from datetime import datetime
from archiver import Config, Transcoder, run_archiver, main


class TestEndToEndWorkflow:
    """End-to-end tests for the complete workflow."""

    def test_complete_workflow_with_transcoding(
        self,
        mock_args,
        camera_dir,
        sample_files,
        archived_dir,
        trash_dir,
        mocker,
        mock_transcode_success,
    ):
        """Test the complete workflow from discovery to transcoding to cleanup."""
        mock_args.directory = str(camera_dir)
        mock_args.output = str(archived_dir)
        mock_args.trash_root = str(trash_dir)
        mock_args.no_confirm = True  # Skip confirmation

        config = Config(mock_args)

        mocker.patch.object(
            Transcoder, "transcode_file", side_effect=mock_transcode_success
        )

        result = run_archiver(config)
        assert result == 0

        # Check that source files were moved to trash
        assert not sample_files["mp4"].exists()
        assert not sample_files["jpg"].exists()

        trash_mp4 = trash_dir / "input" / sample_files["mp4"].relative_to(camera_dir)
        trash_jpg = trash_dir / "input" / sample_files["jpg"].relative_to(camera_dir)
        assert trash_mp4.exists()
        assert trash_jpg.exists()

        # Check that output file was created
        timestamp = sample_files["timestamp"]
        output_file = (
            archived_dir
            / str(timestamp.year)
            / f"{timestamp.month:02d}"
            / f"{timestamp.day:02d}"
            / f"archived-{timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        assert output_file.exists()

    def test_complete_workflow_with_cleanup(
        self, mock_args, camera_dir, sample_files, trash_dir
    ):
        """Test the complete workflow with cleanup mode."""
        # Set up configuration
        mock_args.directory = str(camera_dir)
        mock_args.trash_root = str(trash_dir)
        mock_args.cleanup = True
        mock_args.no_confirm = True  # Skip confirmation

        config = Config(mock_args)

        # Run the archiver
        result = run_archiver(config)

        # Should return 0 for successful execution
        assert result == 0

        # Check that source files were moved to trash
        assert not sample_files["mp4"].exists()
        assert not sample_files["jpg"].exists()

        trash_mp4 = trash_dir / "input" / sample_files["mp4"].relative_to(camera_dir)
        trash_jpg = trash_dir / "input" / sample_files["jpg"].relative_to(camera_dir)
        assert trash_mp4.exists()
        assert trash_jpg.exists()

    def test_complete_workflow_with_dry_run(
        self, mock_args, camera_dir, sample_files, archived_dir, trash_dir
    ):
        """Test the complete workflow in dry-run mode."""
        # Set up configuration
        mock_args.directory = str(camera_dir)
        mock_args.output = str(archived_dir)
        mock_args.trash_root = str(trash_dir)
        mock_args.dry_run = True

        config = Config(mock_args)

        # Run the archiver
        result = run_archiver(config)

        # Should return 0 for successful dry run
        assert result == 0

        # Files should still exist in dry run mode
        assert sample_files["mp4"].exists()
        assert sample_files["jpg"].exists()

        # Output file should not be created
        timestamp = sample_files["timestamp"]
        output_file = (
            archived_dir
            / str(timestamp.year)
            / f"{timestamp.month:02d}"
            / f"{timestamp.day:02d}"
            / f"archived-{timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        assert not output_file.exists()

    def test_complete_workflow_with_existing_archive(
        self,
        mock_args,
        camera_dir,
        sample_files,
        archived_dir,
        trash_dir,
        mocker,
        mock_transcode_success,
    ):
        """Test the complete workflow when archive already exists."""
        # Create an existing archive
        timestamp = sample_files["timestamp"]
        archive_path = (
            archived_dir
            / str(timestamp.year)
            / f"{timestamp.month:02d}"
            / f"{timestamp.day:02d}"
            / f"archived-{timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        archive_path.parent.mkdir(parents=True)
        archive_path.write_bytes(
            b"fake archive"
        )  # non-empty so size > 1 MB check passes

        # Set up configuration
        mock_args.directory = str(camera_dir)
        mock_args.output = str(archived_dir)
        mock_args.trash_root = str(trash_dir)
        mock_args.no_confirm = True
        # do NOT pass --no-skip so the planner will skip transcoding

        config = Config(mock_args)

        # make the (skipped) transcoding succeed anyway so removal is executed
        mocker.patch.object(
            Transcoder, "transcode_file", side_effect=mock_transcode_success
        )

        result = run_archiver(config)
        assert result == 0

        # source files must be gone (skipped transcode but still removed)
        assert not sample_files["mp4"].exists()
        assert not sample_files["jpg"].exists()

        trash_mp4 = trash_dir / "input" / sample_files["mp4"].relative_to(camera_dir)
        trash_jpg = trash_dir / "input" / sample_files["jpg"].relative_to(camera_dir)
        assert trash_mp4.exists()
        assert trash_jpg.exists()

        # Archive should still exist
        assert archive_path.exists()

    def test_complete_workflow_with_multiple_files(
        self,
        mock_args,
        camera_dir,
        archived_dir,
        trash_dir,
        mocker,
        mock_transcode_success,
    ):
        """Test the complete workflow with multiple files."""
        # Create multiple sample files
        timestamps = [
            datetime(2023, 1, 15, 12, 0, 0),
            datetime(2023, 1, 15, 13, 0, 0),
            datetime(2023, 1, 15, 14, 0, 0),
        ]

        sample_files_list = []
        for ts in timestamps:
            # Create directory structure
            year_dir = camera_dir / str(ts.year)
            month_dir = year_dir / f"{ts.month:02d}"
            day_dir = month_dir / f"{ts.day:02d}"
            day_dir.mkdir(parents=True, exist_ok=True)

            # Create sample files
            mp4_file = day_dir / f"REO_camera_{ts.strftime('%Y%m%d%H%M%S')}.mp4"
            jpg_file = day_dir / f"REO_camera_{ts.strftime('%Y%m%d%H%M%S')}.jpg"

            mp4_file.touch()
            jpg_file.touch()

            sample_files_list.append(
                {"mp4": mp4_file, "jpg": jpg_file, "timestamp": ts}
            )

        # Set up configuration
        mock_args.directory = str(camera_dir)
        mock_args.output = str(archived_dir)
        mock_args.trash_root = str(trash_dir)
        mock_args.no_confirm = True  # Skip confirmation

        config = Config(mock_args)

        mocker.patch.object(
            Transcoder, "transcode_file", side_effect=mock_transcode_success
        )

        # Run the archiver
        result = run_archiver(config)

        # Should return 0 for successful execution
        assert result == 0

        # Check that all source files were moved to trash
        for files in sample_files_list:
            assert not files["mp4"].exists()
            assert not files["jpg"].exists()

            trash_mp4 = trash_dir / "input" / files["mp4"].relative_to(camera_dir)
            trash_jpg = trash_dir / "input" / files["jpg"].relative_to(camera_dir)
            assert trash_mp4.exists()
            assert trash_jpg.exists()

            # Check that output file was created
            output_file = (
                archived_dir
                / str(files["timestamp"].year)
                / f"{files['timestamp'].month:02d}"
                / f"{files['timestamp'].day:02d}"
                / f"archived-{files['timestamp'].strftime('%Y%m%d%H%M%S')}.mp4"
            )
            assert output_file.exists()

    def test_complete_workflow_with_signal_interrupt(
        self,
        mock_args,
        camera_dir,
        sample_files,
        archived_dir,
        trash_dir,
        mocker,
        mock_transcode_interrupt,
    ):
        """Test the complete workflow with signal interrupt."""
        # Set up configuration
        mock_args.directory = str(camera_dir)
        mock_args.output = str(archived_dir)
        mock_args.trash_root = str(trash_dir)
        mock_args.no_confirm = True  # Skip confirmation

        config = Config(mock_args)

        mocker.patch.object(
            Transcoder, "transcode_file", side_effect=mock_transcode_interrupt
        )

        # Run the archiver
        result = run_archiver(config)

        # Should return 0 even with interruption
        assert result == 0

        # Files should still exist when interrupted
        assert sample_files["mp4"].exists()
        assert sample_files["jpg"].exists()

    def test_complete_workflow_with_transcoding_failure(
        self,
        mock_args,
        camera_dir,
        sample_files,
        archived_dir,
        trash_dir,
        mocker,
        mock_transcode_fail,
    ):
        """Test the complete workflow with transcoding failure."""
        # Set up configuration
        mock_args.directory = str(camera_dir)
        mock_args.output = str(archived_dir)
        mock_args.trash_root = str(trash_dir)
        mock_args.no_confirm = True  # Skip confirmation

        config = Config(mock_args)

        mocker.patch.object(
            Transcoder, "transcode_file", side_effect=mock_transcode_fail
        )

        # Run the archiver
        result = run_archiver(config)

        # Should return 0 even with transcoding failure
        assert result == 0

        # Files should still exist when transcoding fails
        assert sample_files["mp4"].exists()
        assert sample_files["jpg"].exists()

    def test_main_function(
        self,
        camera_dir,
        sample_files,
        archived_dir,
        trash_dir,
        mocker,
        mock_transcode_success,
    ):
        """Test the main function with command-line arguments."""

        mocker.patch.object(
            Transcoder, "transcode_file", side_effect=mock_transcode_success
        )
        mocker.patch(
            "sys.argv",
            [
                "archiver.py",
                str(camera_dir),
                "-o",
                str(archived_dir),
                "--trash-root",
                str(trash_dir),
                "--no-confirm",
            ],
        )

        result = main()

        # Should return 0 for successful execution
        assert result == 0

        # Check that source files were moved to trash
        assert not sample_files["mp4"].exists()
        assert not sample_files["jpg"].exists()

        trash_mp4 = trash_dir / "input" / sample_files["mp4"].relative_to(camera_dir)
        trash_jpg = trash_dir / "input" / sample_files["jpg"].relative_to(camera_dir)
        assert trash_mp4.exists()
        assert trash_jpg.exists()

        # Check that output file was created
        timestamp = sample_files["timestamp"]
        output_file = (
            archived_dir
            / str(timestamp.year)
            / f"{timestamp.month:02d}"
            / f"{timestamp.day:02d}"
            / f"archived-{timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        assert output_file.exists()
