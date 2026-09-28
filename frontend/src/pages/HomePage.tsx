import { motion, useReducedMotion } from "motion/react";
import Brand from "../components/common/Brand";
import HomeIntro from "../components/home/HomeIntro";
import RoleSelection from "../components/home/RoleSelection";

export default function HomePage() {
  const reduceMotion = useReducedMotion();

  return (
    <main className="grid min-h-screen place-items-center bg-[radial-gradient(circle_at_12%_15%,#f3792926,transparent_30%),radial-gradient(circle_at_90%_86%,#ffb47730,transparent_32%)] p-5">
      <motion.section
        initial={reduceMotion ? false : { opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.3 }}
        className="w-full max-w-4xl rounded-3xl border border-stone-200 bg-white/90 p-7 shadow-2xl shadow-stone-300/40 backdrop-blur md:p-12"
      >
        <Brand />
        <HomeIntro />
        <RoleSelection />
      </motion.section>
    </main>
  );
}
