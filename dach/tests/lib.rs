use std::time::Instant;

use dach::*;

fn lorem_ipsum() -> &'static str {
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec justo eu magna ultrices gravida quis in felis. Mauris ac rutrum enim. Nulla auctor lacus at tellus sagittis dictum non id nunc. Donec ac nisl molestie, egestas dui vitae, consectetur sapien. Vivamus vel aliquet magna, ut malesuada mauris. Curabitur eu erat at lorem rhoncus cursus ac at mauris. Curabitur ullamcorper diam sed leo pellentesque, ac rhoncus quam mattis. Suspendisse potenti. Pellentesque risus ex, egestas in ex nec, sollicitudin accumsan dolor. Donec elementum id odio eget pharetra. Morbi aliquet accumsan vestibulum. Suspendisse eros dui, condimentum sagittis magna non, eleifend egestas dui. Ut pulvinar vestibulum lorem quis laoreet. Nam aliquam ante in placerat volutpat. Sed ac imperdiet ex. Nullam ut neque vel augue dignissim semper.\n\n"
}
fn test_sync(base: &[u8], target: &[u8]) {
    let mut signature = Signature::new(128);
    signature.write(base);
    let signature = signature.finish();

    let diff = signature
        .diff(target)
        .minify(8, base)
        .expect("Failed to minify.");

    println!("Diff {diff:#?}");

    let mut out = Vec::new();
    diff.apply(base, &mut out)
        .expect("Failed to apply good diff.");
    assert_eq!(
        &out,
        target,
        "base didn't become target. Transformed:\n{}\n\nTarget:\n{}",
        String::from_utf8_lossy(&out),
        String::from_utf8_lossy(target)
    );
}

#[test]
fn difference() {
    // This is the data we have
    let local_data =
        "Lorem ipsum dolor sit amet, don't really know Rust elit. Cras nec justo eu magna.";
    // This is the data we want to get.
    let remote_data =
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec justo eu magna.";

    let mut signature = Signature::with_algorithm(HashAlgorithm::XXH3_64, 8);
    signature.write(local_data.as_bytes());
    let signature = signature.finish();

    let now = std::time::Instant::now();
    let diff = signature.diff(remote_data.as_bytes());
    println!("Segments {:#?}", diff.segments());
    println!("Took {:?}", now.elapsed());
    assert_eq!(diff.segments().len(), 3);

    let segment = &diff.segments()[1];
    assert_eq!(
        segment,
        &Segment::unknown(b"et, consectetur adipiscing elit.".as_ref())
    );
}
#[test]
fn block_size_larger_than_input() {
    // This is the data we have
    let local_data = lorem_ipsum().replace("Cras nec justo", "I don't know");
    // This is the data we want to get.
    let remote_data = lorem_ipsum();

    let mut signature = Signature::with_algorithm(HashAlgorithm::XXH3_64, 4096);
    signature.write(local_data.as_bytes());
    let signature = signature.finish();

    let diff = signature.diff(remote_data.as_bytes());
    assert_eq!(diff.segments().len(), 1);
}
#[test]
fn raw_bytes() {
    let local_data = lorem_ipsum().replace("Cras nec justo", "I don't know");
    // This is the data we want to get.
    let remote_data = lorem_ipsum();
    let mut signature = Signature::new(16);
    signature.write(local_data.as_bytes());
    let signature = signature.finish();
    assert_eq!(signature.algorithm(), HashAlgorithm::None16);

    drop(signature.diff(remote_data.as_bytes()));
}
#[test]
fn empty() {
    let local_data = "";
    // This is the data we want to get.
    let remote_data = "";
    let mut signature = Signature::new(512);
    signature.write(local_data.as_bytes());
    let signature = signature.finish();

    println!("Signature: {signature:?}");

    let diff = signature.diff(remote_data.as_bytes());
    assert_eq!(diff.segments(), []);
}
#[test]
fn sync_1() {
    let local_data =
        "Lorem ipsum dolor sit amet, don't really know Rust elit. Cras nec justo eu magna.";
    let remote_data =
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec justo eu magna.";

    test_sync(local_data.as_bytes(), remote_data.as_bytes());
}
#[test]
fn sync_2() {
    test_sync(
        b"Some test data. Hope this test workes, as the whole diff algorithm is written by me!",
        b"Some test data. This test works, as the whole diff algorithm is written by me!",
    );
}
#[test]
fn sync_3() {
    let (old, new) = {
        (
            b"\
hi

well ok
no

Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got binary message.
Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got binary message.
Got text message: 'HI!'
Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got binary message.
Got text message: 'HI!'
Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got binary message.
Got text message: 'HI!'
Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got binary message.
Got binary message.
Got text message: 'HI!'
Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got binary message.
Got binary message.

wowsies!",
            b"\
hi

well ok
no

Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got binary message.
Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got binary message.
Got text message: 'HI!'
Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got binary message.
Got text message: 'HI!'
Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got binary message.
Got text message: 'HI!'
Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got binary message.
Got binary message.
Got text message: 'HI!'
Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got binary message.
Got binary message.

wowsies!
ok",
        )
    };
    test_sync(old, new);
}
#[test]
fn minify() {
    // This is the data we have
    let local_data = lorem_ipsum().replace("Cras nec justo", "I don't know");
    // This is the data we want to get.
    let remote_data = lorem_ipsum();

    let mut signature = Signature::new(32);
    signature.write(local_data.as_bytes());
    let signature = signature.finish();
    println!("Sig {:#?}", signature);

    let diff = signature.diff(remote_data.as_bytes());
    println!("Large diff {:#?}", diff);
    let mut total = 0;
    for seg in diff.segments() {
        if let Segment::Unknown(seg) = seg {
            total += seg.data().len();
        }
    }
    assert_eq!(total, 66);

    let size = diff.approximate_binary_size();

    let diff = diff
        .minify(4, local_data.as_bytes())
        .expect("Failed to minify a correct diff.");
    println!("Minified diff {:#?}", diff);
    let minified_size = diff.approximate_binary_size();
    println!("Original size: {size}, minified size: {minified_size}");
    assert!(minified_size <= size);
    total = 0;
    for seg in diff.segments() {
        if let Segment::Unknown(seg) = seg {
            total += seg.data().len();
        }
    }
    assert_eq!(total, 18);
}
fn revert(old: &[u8], new: &[u8]) {
    let mut sig = dach::Signature::new(8);
    sig.write(old);
    let sig = sig.finish();
    let diff = sig.diff(new);

    let mut buf = vec![];

    diff.apply(old, &mut buf).unwrap();

    assert_eq!(buf, new);

    println!("Diff: {diff:?}");

    let mut reverted = Vec::new();

    diff.revert(&buf, &mut reverted, b' ').unwrap();

    println!("reverted: {:?}", std::str::from_utf8(&reverted));

    let mut target = Vec::new();
    diff.apply(&reverted, &mut target).unwrap();

    println!(
        "Expected: {:?}, got: {:?}",
        std::str::from_utf8(new),
        std::str::from_utf8(&target)
    );
    assert_eq!(target, new);
}
#[test]
fn revert_1() {
    let old = "this is the original text";
    let new = "this was the original text";
    revert(old.as_bytes(), new.as_bytes());
}
#[test]
fn revert_2() {
    let old = "this is the original text";
    let new = "this is some very messed up data changed the original text, which is now gone";
    revert(old.as_bytes(), new.as_bytes());
}

#[test]
fn equal_blocks() {
    let a = "sim testsim testsim testsimtest this is some more data!";
    let b = "sim testsim testsim testsimtest this is some more data!\nNow we're talking!";

    let mut signature = Signature::new(256);
    signature.write(a.as_bytes());
    let signature = signature.finish();
    println!("Sig {:#?}", signature);

    let diff = signature.diff(b.as_bytes());
    println!("Large diff {:#?}", diff);

    let diff = diff
        .minify(8, a.as_bytes())
        .expect("Failed to minify a correct diff.");
    println!("Minified diff {:#?}", diff);

    assert_eq!(
        diff.segments(),
        [
            Segment::Ref(SegmentRef {
                start: 0,
                block_count: 6
            }),
            Segment::unknown("e data!\nNow we're talking!")
        ]
    );
}

#[test]
fn apply_adaptive_end_no_ends() {
    let s1 = "Here we're writing something completely else.";
    let s2 = "This is some data";
    let s3 = "Here we're writing something completely else. i've continued writing";

    let mut s = Signature::new(8);
    s.write(s1.as_bytes());
    let s = s.finish();

    let diff = s.diff(s2.as_bytes());

    assert_eq!(diff.segments().len(), 1);
    assert!(matches!(diff.segments()[0], dach::Segment::Unknown(_)));

    {
        let mut vec = s3.as_bytes().to_vec();
        assert!(!diff.apply_overlaps_adaptive_end(vec.len()));
        diff.apply_in_place_adaptive_end(&mut vec).unwrap();
        assert_eq!(
            std::str::from_utf8(&vec).unwrap(),
            "This is some data i've continued writing"
        );
    }
    {
        let mut other = Vec::new();
        diff.apply_adaptive_end(s3.as_bytes(), &mut other).unwrap();
        assert_eq!(
            std::str::from_utf8(&other).unwrap(),
            "This is some data i've continued writing"
        );
    }
}

#[test]
fn diff_segments_1() {
    // This is the data we have
    let local_data = "wee h test3ch test3ch test 3chok hi hello there my old friend

how are you?";
    // This is the data we want to get.
    let remote_data = "ye! wee h test3ch test3ch test 3chok hi hello there my old friend

how are you?";

    let mut signature = Signature::with_algorithm(HashAlgorithm::CyclicPoly64, 256);
    signature.write(local_data.as_bytes());
    let signature = signature.finish();

    let now = std::time::Instant::now();
    let diff = signature.diff(remote_data.as_bytes());
    let diff = diff.minify(8, local_data.as_bytes()).unwrap();
    println!("Segments {:#?}", diff.segments());
    println!("Took {:?}", now.elapsed());

    assert_eq!(
        diff.segments(),
        [
            Segment::unknown("ye! "),
            Segment::Ref(SegmentRef {
                start: 0,
                block_count: 10
            })
        ]
    )
}

#[test]
fn diff_segments_2() {
    // This is the data we have
    let local_data = "wee h test3ch test3ch test 3chok hi hello there my old friend

how are you? good!";
    // This is the data we want to get.
    let remote_data = "ye! wee h test3ch test3ch test 3chok hi hello there my old friend

hou? good!
ou?";

    let mut signature = Signature::new(256);
    signature.write(local_data.as_bytes());
    let signature = signature.finish();

    let now = std::time::Instant::now();
    let diff = signature.diff(remote_data.as_bytes());
    let diff = diff.minify(8, local_data.as_bytes()).unwrap();
    println!("Segments {:#?}", diff.segments());
    println!("Took {:?}", now.elapsed());

    assert_eq!(
        diff.segments(),
        [
            Segment::unknown("ye! "),
            Segment::Ref(SegmentRef {
                start: 0,
                block_count: 8
            }),
            Segment::Ref(SegmentRef {
                start: 72,
                block_count: 1
            }),
            Segment::unknown("!\nou?")
        ]
    )
}

#[test]
fn parallel_consistency_difference_1() {
    let local_data =
        "Lorem ipsum dolor sit amet, don't really know Rust elit. Cras nec justo eu magna.";
    let remote_data =
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec justo eu magna.";

    let mut signature = Signature::new(8);
    signature.write(local_data.as_bytes());
    let signature = signature.finish();

    let diff = signature.diff(remote_data.as_bytes());
    let parallel_diff = dach::parallel::WORKER_POOL
        .with(|wp| signature.parallel_diff_with_options(remote_data.as_bytes(), wp, 0, 8));
    assert_eq!(diff.segments(), parallel_diff.segments());
}
#[test]
fn parallel_consistency_difference_2() {
    let local_data = str::replacen(
        &lorem_ipsum().repeat(100),
        "Cras nec justo",
        "I don't know",
        7,
    );
    let remote_data = lorem_ipsum().repeat(100);

    let mut signature = Signature::new(128);
    dach::parallel::WORKER_POOL.with(|wp| signature.parallel_write(local_data.as_bytes(), wp));
    let signature = signature.finish();

    // serial
    let now = Instant::now();
    let diff = signature.diff(remote_data.as_bytes());
    println!("Serial took {}µs", now.elapsed().as_micros());

    //parallel
    let now = Instant::now();
    let parallel_diff = dach::parallel::WORKER_POOL
        .with(|wp| signature.parallel_diff_with_options(remote_data.as_bytes(), wp, 0, 512));
    println!("Parallel took {}µs", now.elapsed().as_micros());

    assert_eq!(
        diff.applied_len(local_data.as_bytes()),
        parallel_diff.applied_len(local_data.as_bytes())
    );
    assert_eq!(diff, parallel_diff);

    let mut buf = Vec::new();
    parallel_diff
        .apply(local_data.as_bytes(), &mut buf)
        .unwrap();
    assert_eq!(buf.len(), parallel_diff.applied_len(local_data.as_bytes()));
    assert_eq!(String::from_utf8_lossy(&buf), remote_data);
}
#[test]
fn parallel_consistency_signature_1() {
    let data = str::replace(&lorem_ipsum().repeat(100), "Cras nec justo", "I don't know");

    // serial
    let mut signature = Signature::new(256);
    let now = Instant::now();
    signature.write(data.as_bytes());
    println!("Serial took {}µs", now.elapsed().as_micros());
    //parallel
    let mut parallel_sig = Signature::new(256);
    // make sure it's initiated
    dach::parallel::WORKER_POOL.with(|_wp| {});
    let now = Instant::now();
    dach::parallel::WORKER_POOL
        .with(|wp| parallel_sig.parallel_write_with_options(data.as_bytes(), wp, 0, 0));
    println!("Parallel took {}µs", now.elapsed().as_micros());

    assert_eq!(signature, parallel_sig);

    let signature = signature.finish();
    let parallel_sig = parallel_sig.finish();

    assert_eq!(signature, parallel_sig);
}
