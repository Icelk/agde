use den::*;

fn lorem_ipsum() -> &'static str {
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec justo eu magna ultrices gravida quis in felis. Mauris ac rutrum enim. Nulla auctor lacus at tellus sagittis dictum non id nunc. Donec ac nisl molestie, egestas dui vitae, consectetur sapien. Vivamus vel aliquet magna, ut malesuada mauris. Curabitur eu erat at lorem rhoncus cursus ac at mauris. Curabitur ullamcorper diam sed leo pellentesque, ac rhoncus quam mattis. Suspendisse potenti. Pellentesque risus ex, egestas in ex nec, sollicitudin accumsan dolor. Donec elementum id odio eget pharetra. Morbi aliquet accumsan vestibulum. Suspendisse eros dui, condimentum sagittis magna non, eleifend egestas dui. Ut pulvinar vestibulum lorem quis laoreet. Nam aliquam ante in placerat volutpat. Sed ac imperdiet ex. Nullam ut neque vel augue dignissim semper."
}
fn test_sync(base: &[u8], target: &[u8]) {
    let mut signature = Signature::new(128);
    signature.write(base);
    let signature = signature.finish();

    let diff = signature
        .diff(target)
        .minify(8, base)
        .expect("Failed to minify.");

    let mut out = Vec::new();
    diff.apply(base, &mut out)
        .expect("Failed to apply good diff.");
    assert_eq!(&out, target);
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
    let diff = diff
        .minify(4, local_data.as_bytes())
        .expect("Failed to minify a correct diff.");
    println!("Minified diff {:#?}", diff);
    total = 0;
    for seg in diff.segments() {
        if let Segment::Unknown(seg) = seg {
            total += seg.data().len();
        }
    }
    assert_eq!(total, 18);
}