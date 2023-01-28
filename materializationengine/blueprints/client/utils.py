def add_warnings_to_headers(headers, warnings):
    if len(warnings) > 0:
        headers["Warning"] = ". ".join([w.replace("\n", " ") for w in warnings])
    return headers


def update_notice_text_warnings(ann_md, warnings):
    notice_text = ann_md.get("notice_text", None)
    if notice_text is not None:
        msg = f"Table Owner Warning: {notice_text}"
        warnings.append(msg)

    return warnings
