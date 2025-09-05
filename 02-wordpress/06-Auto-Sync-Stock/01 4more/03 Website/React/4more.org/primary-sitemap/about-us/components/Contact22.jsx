"use client";

import React from "react";
import { BiEnvelope, BiMap, BiMessageDetail, BiPhone } from "react-icons/bi";

export function Contact22() {
  return (
    <section id="relume" className="px-[5%] py-16 md:py-24 lg:py-28">
      <div className="container">
        <div className="grid auto-cols-fr gap-x-8 gap-y-12 sm:gap-x-10 md:grid-cols-2 md:gap-y-16 lg:grid-cols-4">
          <div>
            <div className="mb-5 sm:mb-6">
              <BiEnvelope className="size-12" />
            </div>
            <h3 className="mb-3 text-2xl font-bold leading-[1.4] sm:mb-4 md:text-3xl lg:mb-4 lg:text-4xl">
              Contact
            </h3>
            <p className="mb-5 md:mb-6">
              We’re here to help! Reach out with any questions or concerns.
            </p>
            <a className="underline" href="#">
              info@4more.org
            </a>
          </div>
          <div>
            <div className="mb-5 sm:mb-6">
              <BiMessageDetail className="size-12" />
            </div>
            <h3 className="mb-3 text-2xl font-bold leading-[1.4] sm:mb-4 md:text-3xl lg:mb-4 lg:text-4xl">
              Live Support
            </h3>
            <p className="mb-5 md:mb-6">
              Chat with us for immediate assistance and support.
            </p>
            <a className="underline" href="#">
              Start a chat
            </a>
          </div>
          <div>
            <div className="mb-5 sm:mb-6">
              <BiPhone className="size-12" />
            </div>
            <h3 className="mb-3 text-2xl font-bold leading-[1.4] sm:mb-4 md:text-3xl lg:mb-4 lg:text-4xl">
              Phone
            </h3>
            <p className="mb-5 md:mb-6">
              Call us anytime for quick answers to your inquiries.
            </p>
            <a className="underline" href="#">
              +1 (555) 123-4567
            </a>
          </div>
          <div>
            <div className="mb-5 sm:mb-6">
              <BiMap className="size-12" />
            </div>
            <h3 className="mb-3 text-2xl font-bold leading-[1.4] sm:mb-4 md:text-3xl lg:mb-4 lg:text-4xl">
              Address
            </h3>
            <p className="mb-5 md:mb-6">
              Visit us at our headquarters for any in-person inquiries.
            </p>
            <a className="underline" href="#">
              456 Example Rd, Melbourne VIC 3000 AU
            </a>
          </div>
        </div>
      </div>
    </section>
  );
}
